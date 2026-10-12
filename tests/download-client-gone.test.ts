import type { IncomingMessage, ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import http from 'node:http'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'

import { S3Client } from '@aws-sdk/client-s3'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import * as h3 from 'h3'
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from 'vitest'
import { getDatabase } from '~/lib/db'
import { getStorage, S3Adapter } from '~/lib/storage'

/**
 * A stand-in for the object store: answers every GET with an unending body, so
 * a response only gives its socket back by being read to the end or destroyed.
 * `holdHeaders` keeps requests waiting for headers, like a GetObject queued
 * behind a saturated socket pool.
 */
async function startOrigin({ holdHeaders = false } = {}) {
  const requests: IncomingMessage[] = []
  const server = http.createServer((req, res) => {
    requests.push(req)
    if (holdHeaders) return
    res.writeHead(200, { 'content-type': 'application/octet-stream' })
    const chunk = Buffer.alloc(64 * 1024)
    const pump = () => {
      while (!res.destroyed && res.write(chunk));
    }
    res.on('drain', pump)
    pump()
  })
  server.listen(0)
  await once(server, 'listening')
  const { port } = server.address() as AddressInfo
  return {
    url: `http://localhost:${port}`,
    requests,
    close: async () => {
      server.closeAllConnections()
      server.close()
      await once(server, 'close')
    },
  }
}

function heldSockets(agent: http.Agent) {
  return Object.values(agent.sockets).flat().length
}

// The real route on an in-process h3 app, as in proxy-backpressure.test.ts, so
// the storage it calls and the sockets it holds are observable here.
const router = h3.createRouter({ preemptive: true })
const server = http.createServer(h3.toNodeListener(h3.createApp().use(router)))

beforeAll(async () => {
  for (const [name, value] of Object.entries(h3)) vi.stubGlobal(name, value)
  // Not a static import: the route calls Nitro auto-imports at evaluation time,
  // so it can only load after the stubs above.
  const { default: download } = await import('~/routes/download/[cacheEntryId]')
  router.get('/download/:cacheEntryId', download)
  server.listen(0)
  await once(server, 'listening')
})

afterEach(() => {
  vi.restoreAllMocks()
  server.closeAllConnections()
})

afterAll(async () => {
  vi.unstubAllGlobals()
  server.close()
  await once(server, 'close')
})

async function createMergedEntry() {
  const storage = await getStorage()
  const db = await getDatabase()
  const key = `client-gone-${randomUUID()}`
  const entry = { key, version: 'v1', scope: 'refs/heads/main', repoId: '123' }
  const upload = await storage.createUpload(entry)
  if (!upload) throw new Error('createUpload returned nothing')
  await storage.uploadPart(upload.id, 0, Readable.from('payload'))
  await storage.completeUpload(entry)
  const cacheEntry = await db
    .selectFrom('cache_entries')
    .where('key', '=', key)
    .select(['id', 'locationId'])
    .executeTakeFirstOrThrow()
  // Merged, so download() goes straight to one GetObject on the merged object.
  await db
    .updateTable('storage_locations')
    .set({ mergedAt: Date.now() })
    .where('id', '=', cacheEntry.locationId)
    .execute()
  return cacheEntry
}

/**
 * Regression for the socket leak behind the 2026-10-12 CI hang: a runner gave
 * up on /download while the GetObject sat in the saturated S3 socket queue.
 * The body then came back for a response that was already closed. Node 25's
 * `pipeline` throws ERR_STREAM_UNABLE_TO_PIPE for a closed destination before
 * it takes ownership of the source, and nothing else destroyed the body. Each
 * occurrence pinned one of the agent's sockets (and the reader lease) for the
 * life of the process, until the pool was full and every S3 call queued.
 */
describe('download for a client that left before storage answered', () => {
  test.each([
    // An adapter that cannot cancel its read (GCS, a read already past the
    // abort) hands back a live body: the route must destroy it.
    { adapter: 'ignores the abort signal', honoursSignal: false },
    // The S3 adapter passes the signal to GetObject: the queued request is
    // cancelled before it ever returns a body.
    { adapter: 'honours the abort signal', honoursSignal: true },
  ])(
    'releases the backend socket and reader lease when the adapter $adapter',
    async ({ honoursSignal }) => {
      const origin = await startOrigin()
      const agent = new http.Agent({ keepAlive: true, maxSockets: 1 })
      const storage = await getStorage()
      const db = await getDatabase()
      const { id, locationId } = await createMergedEntry()

      const { promise: clientGone, resolve: markClientGone } = Promise.withResolvers<void>()
      const { promise: adapterSettled, resolve: markAdapterSettled } = Promise.withResolvers<void>()
      let body: IncomingMessage | undefined
      vi.spyOn(storage.adapter, 'createDownloadStream').mockImplementation(
        async (_objectName: string, signal?: AbortSignal) => {
          try {
            await clientGone
            const req = http.get(origin.url, { agent, signal: honoursSignal ? signal : undefined })
            const [res] = (await once(req, 'response')) as [IncomingMessage]
            body = res
            return res
          } finally {
            markAdapterSettled()
          }
        },
      )

      try {
        const { port } = server.address() as AddressInfo
        const serverSide = once(server, 'request') as Promise<[IncomingMessage, ServerResponse]>
        const req = http.get(`http://localhost:${port}/download/${id}`)
        req.on('error', () => {})
        const [, serverRes] = await serverSide
        req.destroy()
        await once(serverRes, 'close')
        markClientGone()
        await adapterSettled

        if (honoursSignal)
          expect(body, 'GetObject cancelled before it returned a body').toBeUndefined()
        else expect(body).toBeDefined()
        await vi.waitFor(
          () => {
            expect(heldSockets(agent), 'sockets held for the gone client').toBe(0)
            if (body) expect(body.destroyed, 'backend body destroyed').toBe(true)
          },
          { timeout: 5000, interval: 25 },
        )
        await vi.waitFor(
          async () => {
            const leases = await db
              .selectFrom('storage_reader_leases')
              .where('storageLocationId', '=', locationId)
              .select('id')
              .execute()
            expect(leases, 'reader leases left behind').toHaveLength(0)
          },
          { timeout: 5000, interval: 25 },
        )
      } finally {
        agent.destroy()
        await origin.close()
      }
    },
    20_000,
  )
})

describe('s3 adapter', () => {
  test('a GetObject waiting for headers is cancelled by the download signal', async () => {
    const origin = await startOrigin({ holdHeaders: true })
    const agent = new http.Agent({ keepAlive: true, maxSockets: 1 })
    const s3 = new S3Client({
      endpoint: origin.url,
      region: 'us-east-1',
      forcePathStyle: true,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      requestHandler: new NodeHttpHandler({ httpAgent: agent }),
    })
    const adapter = new S3Adapter({ s3, bucket: 'vitest' })
    try {
      const controller = new AbortController()
      const download = adapter.createDownloadStream('entry/merged', controller.signal)
      await vi.waitFor(() => expect(origin.requests).toHaveLength(1), { timeout: 5000 })
      controller.abort()

      const outcome = await Promise.race([
        download.then(
          () => 'resolved',
          (err: unknown) => err,
        ),
        sleep(5000, 'still waiting for headers'),
      ])
      expect(outcome).toMatchObject({ name: 'AbortError' })
      await vi.waitFor(() => expect(heldSockets(agent)).toBe(0), { timeout: 5000 })
    } finally {
      s3.destroy()
      agent.destroy()
      await origin.close()
    }
  }, 20_000)
})

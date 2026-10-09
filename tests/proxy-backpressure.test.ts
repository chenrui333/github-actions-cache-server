import type { IncomingMessage, Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import http from 'node:http'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'

import * as h3 from 'h3'
import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { getStorage } from '~/lib/storage'

const MiB = 1024 * 1024
const BODY_BYTES = 64 * MiB
// Kernel socket buffers on both ends of a loopback connection plus stream
// highWaterMarks; anything well above this means the server is buffering the
// body in process memory instead of letting TCP pace the other side.
const MAX_READ_AHEAD = 16 * MiB
const CHUNK = Buffer.alloc(64 * 1024, 1)

// The real route handlers, mounted on an in-process h3 app so the storage layer
// can be replaced by a source/sink whose progress the test can observe.
let server: Server
let baseUrl: string

beforeAll(async () => {
  for (const [name, value] of Object.entries(h3)) vi.stubGlobal(name, value)
  // Dynamic imports on purpose: route modules call Nitro auto-imported globals
  // (`defineEventHandler`, …) at evaluation time, so they can only load after
  // the stubs above are in place.
  const { default: download } = await import('~/routes/download/[cacheEntryId]')
  const { default: upload } = await import('~/routes/devstoreaccount1/upload/[uploadId].put')
  const router = h3
    .createRouter({ preemptive: true }) // as Nitro does: an `undefined` return ends the response
    .get('/download/:cacheEntryId', download)
    .put('/upload/:uploadId', upload)
  server = http.createServer(h3.toNodeListener(h3.createApp().use(router)))
  server.listen(0)
  await once(server, 'listening')
  baseUrl = `http://localhost:${(server.address() as AddressInfo).port}`
})

afterAll(async () => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
  server.closeAllConnections()
  server.close()
  await once(server, 'close')
})

// INFRAOPS-20512: every byte the proxy reads ahead of its peer sits in process
// memory as Buffers. N slow restore clients hold up to N entries' worth; N fast
// uploaders against a slower object store hold up to N request bodies.
describe('proxy streaming backpressure', () => {
  test('download does not read storage ahead of a client that stopped reading', async () => {
    let pulled = 0
    const source = new Readable({
      read() {
        if (pulled >= BODY_BYTES) return this.push(null)
        pulled += CHUNK.length
        this.push(CHUNK)
      },
    })
    const storage = await getStorage()
    vi.spyOn(storage, 'download').mockResolvedValue(source)

    const req = http.get(`${baseUrl}/download/${randomUUID()}`)
    const [res] = (await once(req, 'response')) as [IncomingMessage]
    expect(res.statusCode).toBe(200)
    await once(res, 'data')
    res.pause() // the client stops reading but keeps the connection open

    await sleep(1000)
    const readAhead = pulled
    req.destroy()

    expect(readAhead).toBeLessThan(MAX_READ_AHEAD)
  })

  test('upload does not accept body bytes ahead of what storage consumes', async () => {
    const storage = await getStorage()
    const created = await storage.createUpload({
      key: `backpressure-${randomUUID()}`,
      version: 'v1',
      scope: 'refs/heads/main',
      repoId: '123',
    })
    if (!created) throw new Error('createUpload returned nothing')

    // Storage that stalls (an S3 UploadPart in flight) before draining the body.
    vi.spyOn(storage.adapter, 'uploadStream').mockImplementation(async (_name, stream) => {
      await sleep(1000)
      for await (const _chunk of stream);
    })

    const req = http.request(`${baseUrl}/upload/${created.id}`, {
      method: 'PUT',
      headers: { 'content-length': BODY_BYTES },
    })
    const response = once(req, 'response') as Promise<[IncomingMessage]>
    let accepted = 0
    const writeAll = (async () => {
      while (accepted < BODY_BYTES) {
        accepted += CHUNK.length
        if (!req.write(CHUNK)) await once(req, 'drain')
      }
      req.end()
    })()

    await sleep(500) // storage has not read a byte yet
    const acceptedWhileStalled = accepted

    await writeAll
    const [res] = await response
    res.resume()
    expect(res.statusCode).toBe(201)
    expect(acceptedWhileStalled).toBeLessThan(MAX_READ_AHEAD)
  })
})

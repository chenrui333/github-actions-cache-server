import type { AddressInfo } from 'node:net'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import http from 'node:http'
import { Readable } from 'node:stream'

import * as h3 from 'h3'
import { afterAll, afterEach, beforeAll, expect, test, vi } from 'vitest'
import { getDatabase } from '~/lib/db'
import { getStorage } from '~/lib/storage'

// `generateNumberId` never yields a negative id
const UNKNOWN_UPLOAD_ID = -1

// The real upload route on an in-process h3 app, so storage can be made to fail
// under it.
const router = h3.createRouter({ preemptive: true })
const server = http.createServer(h3.toNodeListener(h3.createApp().use(router)))

beforeAll(async () => {
  for (const [name, value] of Object.entries(h3)) vi.stubGlobal(name, value)
  // Not a static import: the route calls Nitro auto-imports (`defineEventHandler`,
  // …) at evaluation time, so it can only load after the stubs above.
  const { default: upload } = await import('~/routes/devstoreaccount1/upload/[uploadId].put')
  router.put('/upload/:uploadId', upload)
  server.listen(0)
  await once(server, 'listening')
})

afterEach(() => vi.restoreAllMocks())

afterAll(async () => {
  vi.unstubAllGlobals()
  server.close()
  await once(server, 'close')
})

function createUpload() {
  return getStorage().then((storage) =>
    storage.createUpload({
      key: `upload-part-${randomUUID()}`,
      version: 'v1',
      scope: 'refs/heads/main',
      repoId: '123',
    }),
  )
}

test('a block PUT for an unknown upload is refused like a wrong capability', async () => {
  const res = await fetch(
    `http://localhost:3000/devstoreaccount1/upload/${UNKNOWN_UPLOAD_ID}?sig=${'0'.repeat(64)}`,
    { method: 'PUT', body: 'payload' },
  )
  expect(res.status).toBe(403)
})

test('a part for an unknown upload is not written', async () => {
  const storage = await getStorage()
  const uploadStream = vi.spyOn(storage.adapter, 'uploadStream')

  expect(await storage.uploadPart(UNKNOWN_UPLOAD_ID, 0, Readable.from('payload'))).toBe(false)
  expect(uploadStream).not.toHaveBeenCalled()
})

/** `cleanup:uploads` removing the row while a part streams */
async function deleteUploadMidStream(uploadId: number) {
  const [storage, db] = await Promise.all([getStorage(), getDatabase()])
  vi.spyOn(storage.adapter, 'uploadStream').mockImplementation(async (_name, stream) => {
    await db.deleteFrom('uploads').where('id', '=', uploadId).execute()
    for await (const _chunk of stream);
  })
}

test('a part whose upload is deleted mid-stream reports failure', async () => {
  const storage = await getStorage()
  const upload = await createUpload()
  await deleteUploadMidStream(upload.id)

  expect(await storage.uploadPart(upload.id, 0, Readable.from('payload'))).toBe(false)
})

test('an authorized block PUT whose upload is deleted mid-stream answers 404', async () => {
  const upload = await createUpload()
  await deleteUploadMidStream(upload.id)

  const { port } = server.address() as AddressInfo
  const res = await fetch(`http://localhost:${port}/upload/${upload.id}?sig=${upload.capability}`, {
    method: 'PUT',
    body: 'payload',
  })
  expect(res.status).toBe(404)
})

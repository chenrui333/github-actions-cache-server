import type { Env } from '~/lib/schemas'
import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'

import { afterEach, describe, expect, test } from 'vitest'
import { env } from '~/lib/env'
import { Storage } from '~/lib/storage'

const s3Env = env as Extract<Env, { STORAGE_DRIVER: 's3' }>

// The test endpoint is http://, so this also covers the agent NodeHttpHandler
// picks for plain-http endpoints (MinIO, RustFS, in-cluster gateways).
describe.skipIf(process.env.VITEST_STORAGE_DRIVER !== 's3')('s3 socket pool', () => {
  const originalMaxSockets = s3Env.STORAGE_S3_MAX_SOCKETS
  afterEach(() => {
    s3Env.STORAGE_S3_MAX_SOCKETS = originalMaxSockets
  })

  test('the configured pool size bounds the sockets used for an http endpoint', async () => {
    s3Env.STORAGE_S3_MAX_SOCKETS = 2
    const storage = await Storage.fromEnv()
    const folder = `socket-pool-${randomUUID()}`
    for (const name of ['a', 'b', 'c'])
      await storage.adapter.uploadStream(`${folder}/${name}`, Readable.from([Buffer.from(name)]))

    try {
      // An unread body keeps its socket: two of them fill the pool.
      const held = [
        await storage.adapter.createDownloadStream(`${folder}/a`),
        await storage.adapter.createDownloadStream(`${folder}/b`),
      ]
      const third = storage.adapter.createDownloadStream(`${folder}/c`)
      const queued = await Promise.race([third.then(() => false), sleep(1000, true)])
      expect(queued, 'third GET queued for a full pool').toBe(true)

      held[0]!.stream.destroy()
      const { stream } = await third
      expect(Buffer.concat(await stream.toArray()).toString()).toBe('c')
      held[1]!.stream.destroy()
    } finally {
      await storage.adapter.deleteFolder(folder)
    }
  })

  test('a lookup HEAD does not queue behind held reads', async () => {
    s3Env.STORAGE_S3_MAX_SOCKETS = 1
    const storage = await Storage.fromEnv()
    const folder = `socket-pool-${randomUUID()}`
    await storage.adapter.uploadStream(`${folder}/a`, Readable.from([Buffer.from('a')]))

    // A paused restore (or a Merge waiting to upload) keeps its GET socket.
    const held = await storage.adapter.createDownloadStream(`${folder}/a`)
    try {
      const lookup = storage.adapter.objectExists(`${folder}/a`)
      const answered = await Promise.race([lookup, sleep(1000, 'queued')])
      expect(answered, 'HeadObject answered while every data socket is held').toBe(true)
    } finally {
      held.stream.destroy()
      await storage.adapter.deleteFolder(folder)
    }
  })
})

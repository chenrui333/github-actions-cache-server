import { createHash, randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'

import { afterEach, describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import { env } from '~/lib/env'
import { Storage } from '~/lib/storage'

const scope = { version: 'v1', scope: 'refs/heads/main', repoId: '123' }

async function uploadEntry(storage: Storage, parts: Buffer[]) {
  const key = `merge-cap-${randomUUID()}`
  const upload = await storage.createUpload({ key, ...scope })
  for (const [index, part] of parts.entries())
    await storage.uploadPart(upload!.id, index, Readable.from(part))
  await storage.completeUpload({ key, ...scope })
  const db = await getDatabase()
  return db
    .selectFrom('cache_entries')
    .innerJoin('storage_locations', 'storage_locations.id', 'cache_entries.locationId')
    .where('cache_entries.key', '=', key)
    .select(['cache_entries.id as entryId', 'storage_locations.id as locationId', 'folderName'])
    .executeTakeFirstOrThrow()
}

async function sha256(stream: Readable) {
  const hash = createHash('sha256')
  for await (const chunk of stream) hash.update(chunk)
  return hash.digest('hex')
}

describe('merge concurrency limit', () => {
  const originalMaxMerges = env.MAX_CONCURRENT_MERGES
  const originalEagerMerge = env.EAGER_MERGE
  afterEach(() => {
    env.MAX_CONCURRENT_MERGES = originalMaxMerges
    env.EAGER_MERGE = originalEagerMerge
  })

  test('a first restore beyond the limit is served from parts without merging', async () => {
    env.EAGER_MERGE = false
    env.MAX_CONCURRENT_MERGES = 1
    const storage = await Storage.fromEnv()
    const parts = [Buffer.alloc(256 * 1024, 'a'), Buffer.alloc(256 * 1024, 'b')]
    const expected = createHash('sha256').update(Buffer.concat(parts)).digest('hex')
    const first = await uploadEntry(storage, parts)
    const second = await uploadEntry(storage, parts)

    try {
      // The first restore's Merge is paced by its unread stream, so it still
      // holds the only slot when the second restore starts.
      const firstStream = await storage.download(first.entryId)
      const secondStream = await storage.download(second.entryId)
      expect(await sha256(secondStream!)).toBe(expected)
      expect(await sha256(firstStream!)).toBe(expected)
      await storage.waitForOngoingMerges()

      const db = await getDatabase()
      const mergedAt = async (id: string) => {
        const row = await db
          .selectFrom('storage_locations')
          .where('id', '=', id)
          .select('mergedAt')
          .executeTakeFirstOrThrow()
        return row.mergedAt
      }
      expect(await mergedAt(first.locationId)).not.toBeNull()
      expect(await mergedAt(second.locationId)).toBeNull()

      // With the slot free again, the next restore merges the skipped entry.
      expect(await sha256((await storage.download(second.entryId))!)).toBe(expected)
      await storage.waitForOngoingMerges()
      expect(await mergedAt(second.locationId)).not.toBeNull()
    } finally {
      await storage.adapter.deleteFolder(first.folderName)
      await storage.adapter.deleteFolder(second.folderName)
    }
  })
})

import type { Readable } from 'node:stream'
import type { Storage } from '~/lib/storage'
import { Buffer } from 'node:buffer'
import { getDatabase } from '~/lib/db'

export interface FixtureUpload {
  id: number
  capability: string
}

/** Fixed-width opaque IDs give existing storage fixtures an explicit order. */
export function stageFixturePart(
  storage: Storage,
  upload: FixtureUpload,
  index: number,
  body: Readable,
) {
  const id = Buffer.from(String(index).padStart(12, '0')).toString('base64')
  return storage.stageBlock(upload.id, upload.capability, id, body)
}

export async function completeFixtureUpload(storage: Storage, upload: FixtureUpload) {
  const db = await getDatabase()
  const staged = await db
    .selectFrom('upload_parts')
    .select('blockId')
    .where('uploadId', '=', upload.id)
    .execute()
  const ids = [...new Set(staged.map(({ blockId }) => blockId!))].toSorted((a, b) =>
    Buffer.from(a, 'base64').compare(Buffer.from(b, 'base64')),
  )
  return storage.commitBlockList(
    upload.id,
    upload.capability,
    ids.map((id) => ({ id, source: 'Latest' })),
  )
}

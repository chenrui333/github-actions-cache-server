import type { Kysely, Transaction } from 'kysely'
import type { Readable } from 'node:stream'
import type { Database, StorageLocation, UploadPart, UploadReceipt } from './db'
import type { StorageAdapter } from './storage'
import type { BlockReference, UploadIdentity } from './upload-protocol'
import { Buffer } from 'node:buffer'
import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto'
import { Readable as NodeReadable } from 'node:stream'
import { sql } from 'kysely'
import { retryOnLockConflict } from './db'
import { env } from './env'
import { generateNumberId } from './helpers'
import { UploadProtocolError, validateBlockId } from './upload-protocol'

// GitHub documents a five-day self-hosted job limit. Seven days adds two days
// of retirement margin; it is NOT a generic lease or a JWT-expiry assertion.
// https://docs.github.com/en/actions/reference/limits
const LEGACY_RETIREMENT_MS = 7 * 24 * 60 * 60 * 1000
const ATTEMPT_LEASE_MS = 60_000
const ATTEMPT_RENEW_MS = 10_000

function assertPublicationAuthorized(upload: { publicationExpiresAt: number | null }) {
  if (upload.publicationExpiresAt === null) {
    if (!env.SKIP_TOKEN_VALIDATION)
      throw new UploadProtocolError(403, 'Upload requires the approved pre-upgrade drain boundary')
  } else if (
    !Number.isFinite(upload.publicationExpiresAt) ||
    Date.now() >= upload.publicationExpiresAt
  )
    throw new UploadProtocolError(403, 'Upload publication authorization expired')
}

/** Caller holds the upload lock. Expiry fences the exact attempt, not other workers. */
export async function recoverExpiredUploadParts(tx: Transaction<Database>, uploadId: number) {
  const removed = await tx
    .deleteFrom('upload_parts')
    .where('uploadId', '=', uploadId)
    .where('sizeBytes', 'is', null)
    .where('leaseExpiresAt', '<=', Date.now())
    .executeTakeFirst()
  const count = Number(removed.numDeletedRows)
  if (count > 0)
    await tx
      .updateTable('uploads')
      .set({ finishedPartUploadCount: sql`${sql.ref('finishedPartUploadCount')} + ${count}` })
      .where('id', '=', uploadId)
      .execute()
  return count
}

export interface UploadPublication {
  receipt: UploadReceipt
  location?: StorageLocation
  sizes: number[]
}

function capabilityDigest(capability: string | undefined) {
  if (!capability) throw new UploadProtocolError(403, 'Forbidden')
  return createHash('sha256').update(capability).digest('hex')
}

function sameCapability(stored: string | null, digest: string) {
  return stored !== null && timingSafeEqual(Buffer.from(stored, 'hex'), Buffer.from(digest, 'hex'))
}

function receipt(db: Kysely<Database>, uploadId: number, digest: string) {
  return db
    .selectFrom('upload_receipts')
    .selectAll()
    .where('uploadId', '=', uploadId)
    .where('capabilityHash', '=', digest)
    .executeTakeFirst()
}

async function lockUpload(tx: Transaction<Database>, uploadId: number, digest: string) {
  let query = tx.selectFrom('uploads').selectAll().where('id', '=', uploadId)
  if (env.DB_DRIVER !== 'sqlite') query = query.forUpdate()
  const upload = await query.executeTakeFirst()
  if (!upload || !sameCapability(upload.capabilityHash, digest))
    throw new UploadProtocolError(403, 'Forbidden')
  return upload
}

export async function authorizeUpload(
  db: Kysely<Database>,
  uploadId: number,
  capability: string | undefined,
) {
  if (!capability) return false
  const digest = capabilityDigest(capability)
  const upload = await db
    .selectFrom('uploads')
    .select('capabilityHash')
    .where('id', '=', uploadId)
    .executeTakeFirst()
  if (upload && sameCapability(upload.capabilityHash, digest)) return true
  return !!(await receipt(db, uploadId, digest))
}

async function receivePart(
  db: Kysely<Database>,
  adapter: StorageAdapter,
  uploadId: number,
  digest: string,
  blockId: string | null,
  stream: Readable,
) {
  const { upload, part } = await retryOnLockConflict(() =>
    db
      .transaction()
      .setIsolationLevel('serializable')
      .execute(async (tx) => {
        if (await receipt(tx, uploadId, digest))
          throw new UploadProtocolError(409, 'Upload is already committed')
        const upload = await lockUpload(tx, uploadId, digest)
        assertPublicationAuthorized(upload)
        await recoverExpiredUploadParts(tx, uploadId)
        if (blockId !== null) {
          const first = await tx
            .selectFrom('upload_parts')
            .select('blockId')
            .where('uploadId', '=', uploadId)
            .where('blockId', 'is not', null)
            .executeTakeFirst()
          if (first?.blockId && validateBlockId(first.blockId) !== validateBlockId(blockId))
            throw new UploadProtocolError(
              400,
              'Block IDs in one upload must have the same decoded length',
            )
        }
        const part: UploadPart = {
          id: randomUUID(),
          uploadId,
          blockId,
          revision: upload.startedPartUploadCount + 1,
          partName: randomUUID(),
          sizeBytes: null,
          digest: null,
          leaseExpiresAt: Date.now() + ATTEMPT_LEASE_MS,
        }
        await tx.insertInto('upload_parts').values(part).execute()
        await tx
          .updateTable('uploads')
          .set({ startedPartUploadCount: part.revision })
          .where('id', '=', uploadId)
          .execute()
        return { upload, part }
      }),
  )
  let sizeBytes = 0
  const hash = blockId === null ? createHash('sha256') : undefined
  const measured = NodeReadable.from(
    (async function* () {
      for await (const chunk of stream) {
        sizeBytes += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.byteLength
        hash?.update(chunk)
        yield chunk
      }
    })(),
  )
  let renewal: Promise<void> | undefined
  const leaseTimer = setInterval(() => {
    renewal ??= retryOnLockConflict(() =>
      db.transaction().execute(async (tx) => {
        await lockUpload(tx, uploadId, digest)
        const now = Date.now()
        const updated = await tx
          .updateTable('upload_parts')
          .set({ leaseExpiresAt: now + ATTEMPT_LEASE_MS })
          .where('id', '=', part.id)
          .where('uploadId', '=', uploadId)
          .where('sizeBytes', 'is', null)
          .where('leaseExpiresAt', '>', now)
          .executeTakeFirst()
        if (Number(updated.numUpdatedRows) !== 1)
          throw new UploadProtocolError(409, 'Upload attempt lease expired')
      }),
    )
      .catch((err: unknown) => {
        measured.destroy(
          err instanceof Error ? err : new Error('Upload attempt lease renewal failed'),
        )
      })
      .finally(() => {
        renewal = undefined
      })
  }, ATTEMPT_RENEW_MS)
  leaseTimer.unref()
  const stopRenewal = async () => {
    clearInterval(leaseTimer)
    await renewal
  }
  try {
    await adapter.uploadStream(`${upload.folderName}/parts/${part.partName}`, measured)
    await stopRenewal()
    const finished = { ...part, sizeBytes, digest: hash?.digest('hex') ?? null }
    await retryOnLockConflict(() =>
      db.transaction().execute(async (tx) => {
        assertPublicationAuthorized(await lockUpload(tx, uploadId, digest))
        const updated = await tx
          .updateTable('upload_parts')
          .set({ sizeBytes, digest: finished.digest })
          .where('id', '=', part.id)
          .where('uploadId', '=', uploadId)
          .where('sizeBytes', 'is', null)
          .where('leaseExpiresAt', '>', Date.now())
          .executeTakeFirst()
        if (Number(updated.numUpdatedRows) !== 1)
          throw new UploadProtocolError(409, 'Upload attempt lease expired')
        await tx
          .updateTable('uploads')
          .set({
            lastPartUploadedAt: Date.now(),
            finishedPartUploadCount: sql`${sql.ref('finishedPartUploadCount')} + 1`,
          })
          .where('id', '=', uploadId)
          .execute()
      }),
    )
    return finished
  } catch (err) {
    await stopRenewal()
    measured.destroy()
    // An unsuccessful immutable revision is not eligible for a manifest.
    // Physical debris remains under the existing orphan/parts lifecycle.
    try {
      await retryOnLockConflict(() =>
        db.transaction().execute(async (tx) => {
          await lockUpload(tx, uploadId, digest)
          const deleted = await tx
            .deleteFrom('upload_parts')
            .where('id', '=', part.id)
            .where('sizeBytes', 'is', null)
            .executeTakeFirst()
          if (Number(deleted.numDeletedRows) === 1)
            // Settled attempts are not completion proof; only the explicit
            // manifest's successfully persisted revisions can be published.
            await tx
              .updateTable('uploads')
              .set({
                lastPartUploadedAt: Date.now(),
                finishedPartUploadCount: sql`${sql.ref('finishedPartUploadCount')} + 1`,
              })
              .where('id', '=', uploadId)
              .execute()
        }),
      )
    } catch (err_) {
      throw new AggregateError([err, err_], 'Upload and attempt settlement failed')
    }
    throw err
  }
}

export async function stageBlock(
  db: Kysely<Database>,
  adapter: StorageAdapter,
  uploadId: number,
  capability: string | undefined,
  blockId: string,
  stream: Readable,
) {
  validateBlockId(blockId)
  await receivePart(db, adapter, uploadId, capabilityDigest(capability), blockId, stream)
}

export async function putBlob(
  db: Kysely<Database>,
  adapter: StorageAdapter,
  uploadId: number,
  capability: string | undefined,
  stream: Readable,
): Promise<UploadPublication> {
  const digest = capabilityDigest(capability)
  const prior = await receipt(db, uploadId, digest)
  if (prior) {
    const hash = createHash('sha256')
    let size = 0
    for await (const chunk of stream) {
      size += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.byteLength
      hash.update(chunk)
    }
    if (
      prior.kind !== 'whole' ||
      prior.sizeBytes !== size ||
      prior.completionHash !== hash.digest('hex')
    )
      throw new UploadProtocolError(409, 'Completed upload is immutable')
    return { receipt: prior, sizes: [] }
  }
  const part = await receivePart(db, adapter, uploadId, digest, null, stream)
  return publishUpload(db, uploadId, digest, 'whole', part.digest!, [], part.id)
}

export async function commitBlockList(
  db: Kysely<Database>,
  uploadId: number,
  capability: string | undefined,
  blocks: BlockReference[],
) {
  if (blocks.length > 50_000) throw new UploadProtocolError(400, 'Too many blocks')
  const sources = new Map<string, BlockReference['source']>()
  let length: number | undefined
  for (const block of blocks) {
    const size = validateBlockId(block.id)
    if (length !== undefined && size !== length)
      throw new UploadProtocolError(400, 'Block IDs must have the same decoded length')
    if (sources.has(block.id) && sources.get(block.id) !== block.source)
      throw new UploadProtocolError(400, 'Repeated block IDs must use the same source list')
    sources.set(block.id, block.source)
    length = size
  }
  const hash = createHash('sha256')
    .update(JSON.stringify(blocks.map(({ id }) => id)))
    .digest('hex')
  return publishUpload(db, uploadId, capabilityDigest(capability), 'blocks', hash, blocks)
}

async function publishUpload(
  db: Kysely<Database>,
  uploadId: number,
  digest: string,
  kind: UploadReceipt['kind'],
  completionHash: string,
  blocks: BlockReference[],
  wholePartId?: string,
): Promise<UploadPublication> {
  return retryOnLockConflict(() =>
    db
      .transaction()
      .setIsolationLevel('serializable')
      .execute(async (tx) => {
        const prior = await receipt(tx, uploadId, digest)
        if (prior) {
          if (prior.kind !== kind || prior.completionHash !== completionHash)
            throw new UploadProtocolError(409, 'Completed upload is immutable')
          return { receipt: prior, sizes: [] }
        }
        const upload = await lockUpload(tx, uploadId, digest)
        // Recheck after all body/storage I/O, under the visibility transaction.
        assertPublicationAuthorized(upload)
        await recoverExpiredUploadParts(tx, uploadId)
        const staged = await tx
          .selectFrom('upload_parts')
          .selectAll()
          .where('uploadId', '=', uploadId)
          .orderBy('revision', 'desc')
          .execute()
        if (staged.some((part) => part.sizeBytes === null))
          throw new UploadProtocolError(409, 'Part writes are still in progress')
        const latest = new Map<string, UploadPart>()
        for (const part of staged)
          if (part.blockId !== null && !latest.has(part.blockId)) latest.set(part.blockId, part)
        const selected: UploadPart[] = []
        if (kind === 'whole') {
          const part = staged.find(({ id }) => id === wholePartId)
          if (!part || part.blockId !== null || part.digest !== completionHash)
            throw new UploadProtocolError(409, 'Whole upload revision is unavailable')
          selected.push(part)
        } else
          for (const block of blocks) {
            const part = latest.get(block.id)
            if (!part || block.source === 'Committed')
              throw new UploadProtocolError(400, 'Manifest references an unavailable block')
            selected.push(part)
          }
        const sizes = selected.map(({ sizeBytes }) => sizeBytes!)
        const location: StorageLocation = {
          id: randomUUID(),
          folderName: upload.folderName,
          partCount: selected.length,
          partNamesJson: JSON.stringify(selected.map(({ partName }) => partName)),
          mergedAt: null,
          mergeStartedAt: null,
          partsDeletedAt: null,
          lastDownloadedAt: null,
          sizeBytes: sizes.reduce((sum, size) => sum + size, 0),
        }
        if (!Number.isSafeInteger(location.sizeBytes))
          throw new UploadProtocolError(400, 'Completed upload is too large')
        await tx.insertInto('storage_locations').values(location).execute()
        const existing = await tx
          .selectFrom('cache_entries')
          .select('id')
          .where('key', '=', upload.key)
          .where('version', '=', upload.version)
          .where('scope', '=', upload.scope)
          .where('repoId', '=', upload.repoId)
          .executeTakeFirst()
        if (existing)
          await tx
            .updateTable('cache_entries')
            .set({ updatedAt: Date.now(), locationId: location.id })
            .where('id', '=', existing.id)
            .execute()
        else
          await tx
            .insertInto('cache_entries')
            .values({
              id: randomUUID(),
              key: upload.key,
              version: upload.version,
              scope: upload.scope,
              repoId: upload.repoId,
              updatedAt: Date.now(),
              locationId: location.id,
            })
            .execute()
        const completed: UploadReceipt = {
          uploadId,
          capabilityHash: digest,
          locationId: location.id,
          kind,
          completionHash,
          sizeBytes: location.sizeBytes!,
        }
        await tx.insertInto('upload_receipts').values(completed).execute()
        await tx.deleteFrom('uploads').where('id', '=', uploadId).execute()
        assertPublicationAuthorized(upload)
        return { receipt: completed, location, sizes }
      }),
  )
}

/** Key-only Finalize is read-only: it cannot touch a pending generation. */
export function acknowledgeUpload(
  db: Kysely<Database>,
  { key, version, scope, repoId }: UploadIdentity,
) {
  return db
    .selectFrom('cache_entries')
    .innerJoin('upload_receipts', 'upload_receipts.locationId', 'cache_entries.locationId')
    .select(['upload_receipts.uploadId', 'upload_receipts.sizeBytes'])
    .where('key', '=', key)
    .where('version', '=', version)
    .where('scope', '=', scope)
    .where('repoId', '=', repoId)
    .executeTakeFirst()
}

export async function createUpload(
  db: Kysely<Database>,
  {
    key,
    version,
    scope,
    repoId,
    publicationExpiresAt = null,
  }: UploadIdentity & {
    publicationExpiresAt?: number | null
  },
) {
  return retryOnLockConflict(() =>
    db
      .transaction()
      .setIsolationLevel('serializable')
      .execute(async (tx) => {
        const now = Date.now()
        const matches = await tx
          .selectFrom('uploads')
          .selectAll()
          .where('key', '=', key)
          .where('version', '=', version)
          .where('scope', '=', scope)
          .where('repoId', '=', repoId)
          .limit(2)
          .execute()
        if (matches.length > 1) return
        const existing = matches[0]
        if (existing) {
          // Requested-key legacy recovery, NOT global cleanup or modern expiry.
          // NULL-capability URLs cannot write; read-only Finalize cannot mutate
          // their successor. Do not confuse global cleanup's 60s idle scan with
          // safe retirement of a client's reservation.
          if (
            existing.capabilityHash !== null ||
            existing.createdAt >= now - LEGACY_RETIREMENT_MS ||
            (existing.lastPartUploadedAt !== null &&
              existing.lastPartUploadedAt >= now - LEGACY_RETIREMENT_MS) ||
            existing.startedPartUploadCount !== existing.finishedPartUploadCount
          )
            return
          const completed = await tx
            .selectFrom('cache_entries')
            .select('id')
            .where('key', '=', key)
            .where('version', '=', version)
            .where('scope', '=', scope)
            .where('repoId', '=', repoId)
            .executeTakeFirst()
          const location = await tx
            .selectFrom('storage_locations')
            .select('id')
            .where('folderName', '=', existing.folderName)
            .executeTakeFirst()
          const pending = await tx
            .selectFrom('upload_parts')
            .select('id')
            .where('uploadId', '=', existing.id)
            .where('sizeBytes', 'is', null)
            .executeTakeFirst()
          if (completed || location || pending) return
          await tx
            .deleteFrom('uploads')
            .where('id', '=', existing.id)
            .where('capabilityHash', 'is', null)
            .execute()
        }
        const id = generateNumberId()
        const capability = randomBytes(32).toString('hex')
        await tx
          .insertInto('uploads')
          .values({
            id,
            folderName: `${id}-${randomUUID()}`,
            createdAt: now,
            key,
            version,
            scope,
            repoId,
            lastPartUploadedAt: null,
            finishedPartUploadCount: 0,
            startedPartUploadCount: 0,
            capabilityHash: capabilityDigest(capability),
            publicationExpiresAt,
          })
          .execute()
        return { id, capability }
      }),
  )
}

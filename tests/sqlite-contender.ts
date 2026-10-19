// One contending process for sqlite-multiprocess.test.ts, run with tsx. Opens
// the shared database through getDatabase() (pragmas, migration lock) and runs
// the server's write paths against rows every contender shares, then prints one
// `RESULT {json}` line.
import { randomUUID } from 'node:crypto'
import { setImmediate as nextMacrotask } from 'node:timers/promises'
import { sql } from 'kysely'
import { getDatabase } from '~/lib/db'
import {
  acquireMergeLease,
  createReaderLease,
  releaseMergeLease,
  releaseReaderLease,
} from '~/lib/storage-leases'
import { deleteStorageLocationIfUnread } from '~/lib/storage-lifecycle'

// Keep in sync with sqlite-multiprocess.test.ts.
const LEASED_LOCATION = 'contended-merge-lease'
const COUNTER_LOCATION = 'contended-transaction'
const READ_LOCATION = 'contended-reader-lease'

const iterations = Number(process.env.CONTENDER_ITERATIONS ?? 200)

const db = await getDatabase()

async function ensureLocation(id: string) {
  await db
    .insertInto('storage_locations')
    .values({
      id,
      folderName: id,
      partCount: 0,
      mergeStartedAt: null,
      mergedAt: null,
      partsDeletedAt: null,
      lastDownloadedAt: null,
      sizeBytes: 0,
    })
    .onConflict((conflict) => conflict.column('id').doNothing())
    .execute()
}

await ensureLocation(LEASED_LOCATION)
await ensureLocation(COUNTER_LOCATION)
await ensureLocation(READ_LOCATION)

async function pragma(name: string) {
  const { rows } = await sql.raw<Record<string, unknown>>(`pragma ${name}`).execute(db)
  return rows[0] && Object.values(rows[0])[0]
}

const result = {
  pragmas: {
    journalMode: await pragma('journal_mode'),
    synchronous: await pragma('synchronous'),
    busyTimeout: await pragma('busy_timeout'),
  },
  leasesAcquired: 0,
  transactions: 0,
  readerLeases: 0,
  locationDeletes: 0,
  uploads: 0,
  violations: [] as string[],
  errors: [] as string[],
}

async function step(run: () => Promise<void>) {
  try {
    await run()
  } catch (err) {
    const { code, message } = err as { code?: string; message?: string }
    result.errors.push(code ?? message ?? String(err))
  }
}

for (let i = 0; i < iterations; i++) {
  // Merge Lease: a non-atomic read-modify-write is only safe under mutual
  // exclusion, so a lost increment means two processes held the lease.
  await step(async () => {
    const token = await acquireMergeLease(db, LEASED_LOCATION)
    if (!token) return
    const { partCount } = await db
      .selectFrom('storage_locations')
      .select('partCount')
      .where('id', '=', LEASED_LOCATION)
      .executeTakeFirstOrThrow()
    await nextMacrotask()
    await db
      .updateTable('storage_locations')
      .set({ partCount: partCount + 1 })
      .where('id', '=', LEASED_LOCATION)
      .execute()
    result.leasesAcquired++
    await releaseMergeLease(db, LEASED_LOCATION, token)
  })

  // Read-then-write transaction, the shape of the lease-fenced merge completion:
  // the classic deferred-transaction SQLITE_BUSY that busy_timeout cannot absorb.
  await step(async () => {
    await db.transaction().execute(async (tx) => {
      const { sizeBytes } = await tx
        .selectFrom('storage_locations')
        .select('sizeBytes')
        .where('id', '=', COUNTER_LOCATION)
        .executeTakeFirstOrThrow()
      await tx
        .updateTable('storage_locations')
        .set({ sizeBytes: (sizeBytes ?? 0) + 1 })
        .where('id', '=', COUNTER_LOCATION)
        .execute()
    })
    result.transactions++
  })

  // Storage Reader Lease vs deletion (ADR-0002): a location must survive while
  // a lease taken in the same transaction that saw it is active.
  await step(async () => {
    const leaseId = await db.transaction().execute(async (tx) => {
      const location = await tx
        .selectFrom('storage_locations')
        .select('id')
        .where('id', '=', READ_LOCATION)
        .executeTakeFirst()
      if (!location) return
      return createReaderLease(tx, location.id, 'storage')
    })
    if (!leaseId) return
    result.readerLeases++
    await nextMacrotask()
    const location = await db
      .selectFrom('storage_locations')
      .select('id')
      .where('id', '=', READ_LOCATION)
      .executeTakeFirst()
    if (!location) result.violations.push('location deleted under an active reader lease')
    await releaseReaderLease(db, leaseId)
  })
  await step(async () => {
    const deleted = await db
      .transaction()
      .execute((tx) => deleteStorageLocationIfUnread(tx, READ_LOCATION))
    if (!deleted) return
    result.locationDeletes++
    await ensureLocation(READ_LOCATION)
  })

  // Autocommit writes, the shape of upload bookkeeping.
  await step(async () => {
    const id = Number.parseInt(randomUUID().replaceAll(/\D/g, '').slice(0, 12) || '1')
    await db
      .insertInto('uploads')
      .values({
        id,
        key: `contender-${process.pid}`,
        version: 'v',
        scope: 's',
        repoId: 'r',
        createdAt: Date.now(),
        lastPartUploadedAt: null,
        startedPartUploadCount: 0,
        finishedPartUploadCount: 0,
        folderName: String(id),
      })
      .execute()
    await db.deleteFrom('uploads').where('id', '=', id).execute()
    result.uploads++
  })
}

await db.destroy()
// eslint-disable-next-line no-console
console.log(`RESULT ${JSON.stringify(result)}`)

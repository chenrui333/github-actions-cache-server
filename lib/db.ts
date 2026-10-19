import type { DatabaseConnection, SqliteDialectConfig } from 'kysely'
import { mkdir } from 'node:fs/promises'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { createSingletonPromise } from '@antfu/utils'
import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import {
  CompiledQuery,
  Kysely,
  MysqlDialect,
  PostgresDialect,
  sql,
  SqliteDialect,
  SqliteDriver,
} from 'kysely'
import { Migrator } from 'kysely/migration'
import { createPool } from 'mysql2'
import pg from 'pg'
import { match } from 'ts-pattern'
import z from 'zod'
import { env } from './env'
import { logger } from './logger'
import { migrations } from './migrations'

// we only really use bigint for timestamps, so we can safely parse them as numbers
pg.types.setTypeParser(20, Number)

export const cacheEntrySchema = z.object({
  id: z.string(),
  key: z.string(),
  version: z.string(),
  scope: z.string(),
  repoId: z.string(),
  updatedAt: z.number(),
  locationId: z.string(),
})
type CacheEntry = z.infer<typeof cacheEntrySchema>

export const storageLocationSchema = z.object({
  id: z.string(),
  folderName: z.string(),
  partCount: z.number(),
  mergeStartedAt: z.number().nullable(),
  mergedAt: z.number().nullable(),
  partsDeletedAt: z.number().nullable(),
  lastDownloadedAt: z.number().nullable(),
  sizeBytes: z.number().nullable(),
})
export type StorageLocation = z.infer<typeof storageLocationSchema>

export const mergeLeaseSchema = z.object({
  storageLocationId: z.string(),
  token: z.string(),
  expiresAt: z.number(),
})
type MergeLease = z.infer<typeof mergeLeaseSchema>

export const storageReaderLeaseScopeSchema = z.enum(['parts', 'storage'])
export type StorageReaderLeaseScope = z.infer<typeof storageReaderLeaseScopeSchema>

export const storageReaderLeaseSchema = z.object({
  id: z.string(),
  storageLocationId: z.string(),
  scope: storageReaderLeaseScopeSchema,
  expiresAt: z.number(),
})
type StorageReaderLease = z.infer<typeof storageReaderLeaseSchema>

export const uploadSchema = z.object({
  id: z.number(),
  key: z.string(),
  version: z.string(),
  scope: z.string(),
  repoId: z.string(),
  createdAt: z.number(),
  lastPartUploadedAt: z.number().nullable(),
  startedPartUploadCount: z.number(),
  finishedPartUploadCount: z.number(),
  folderName: z.string(),
})
type Upload = z.infer<typeof uploadSchema>

export interface Database {
  cache_entries: CacheEntry
  merge_leases: MergeLease
  storage_reader_leases: StorageReaderLease
  storage_locations: StorageLocation
  uploads: Upload
}

const dbLogger = logger.withTag('db')

// mysql2 `ER_LOCK_DEADLOCK` / `ER_LOCK_WAIT_TIMEOUT`, and the SQL states for
// postgres `deadlock_detected` / `serialization_failure`.
const RETRYABLE_LOCK_ERRORS = new Set([
  'ER_LOCK_DEADLOCK',
  'ER_LOCK_WAIT_TIMEOUT',
  '40001',
  '40P01',
])

/** better-sqlite3 reports extended codes: SQLITE_BUSY, SQLITE_BUSY_SNAPSHOT, ... */
function isSqliteBusy(err: unknown) {
  const code = (err as { code?: unknown } | null)?.code
  return typeof code === 'string' && code.startsWith('SQLITE_BUSY')
}

function isRetryableLockError(err: unknown) {
  const code = (err as { code?: unknown } | null)?.code
  return typeof code === 'string' && (RETRYABLE_LOCK_ERRORS.has(code) || isSqliteBusy(err))
}

/**
 * Runs a transaction, retrying it whole if the database picks it as a deadlock
 * victim. Concurrent writers take row locks on `storage_locations` and the lease
 * tables in differing orders, so a deadlock is expected rather than exceptional
 * — the loser has to start over. On SQLite it also retries a transaction that
 * waited out busy_timeout. Only wrap transactions that are safe to repeat.
 */
export async function retryOnLockConflict<T>(run: () => Promise<T>, attempts = 3) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await run()
    } catch (err) {
      if (attempt >= attempts || !isRetryableLockError(err)) throw err
      dbLogger.warn(`Retrying transaction after lock conflict (attempt ${attempt})`, { error: err })
    }
  }
}

/**
 * How long one statement waits for another process's SQLite write lock before
 * failing with SQLITE_BUSY. better-sqlite3 waits synchronously, so this also
 * bounds how long a worker's event loop can stall. Write transactions never
 * await I/O while holding the lock, so real waits are far shorter. See ADR-0010.
 */
const SQLITE_BUSY_TIMEOUT_MS = 5000
/** Upper bound for startup steps that wait on another process (WAL switch, migrations). */
const SQLITE_STARTUP_LOCK_TIMEOUT_MS = 120_000
/** Size the WAL file is truncated back to after a checkpoint resets it. */
const SQLITE_JOURNAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024

async function retrySqliteBusyUntil<T>(deadline: number, run: () => T | Promise<T>) {
  for (;;) {
    try {
      return await run()
    } catch (err) {
      if (!isSqliteBusy(err) || Date.now() >= deadline) throw err
      await sleep(100)
    }
  }
}

/**
 * Opens the SQLite file. The journal mode is a persistent property of the file
 * and is only changed when `DB_SQLITE_JOURNAL_MODE` asks for it, so by default
 * an existing database keeps its mode (SQLite creates new files in rollback
 * journal mode). `wal` lets the workers of a `node-cluster` deployment share
 * the file: readers never block the single writer, and the writer never blocks
 * readers. It pairs with `synchronous=NORMAL`: a process crash loses nothing, a
 * power loss or kernel crash can roll back the last commits before the next
 * checkpoint but never corrupts the file. See ADR-0010.
 */
async function openSqlite(file: string, journalMode: 'wal' | 'delete' | undefined) {
  await mkdir(path.dirname(file), { recursive: true })
  const database = new SQLite(file, { timeout: SQLITE_BUSY_TIMEOUT_MS })
  database.pragma(`busy_timeout = ${SQLITE_BUSY_TIMEOUT_MS}`)
  if (!journalMode) return database
  // Switching needs a moment of exclusive access, which another process
  // starting at the same time may hold: SQLite then fails with SQLITE_BUSY or
  // returns the unchanged mode.
  const deadline = Date.now() + SQLITE_STARTUP_LOCK_TIMEOUT_MS
  for (;;) {
    const mode: unknown = await retrySqliteBusyUntil(deadline, (): unknown =>
      database.pragma(`journal_mode = ${journalMode}`, { simple: true }),
    )
    if (mode === journalMode) break
    if (Date.now() >= deadline) {
      database.close()
      throw new Error(
        `SQLite did not switch to the ${journalMode} journal mode (got ${String(mode)})`,
      )
    }
    await sleep(100)
  }
  if (journalMode === 'wal') {
    database.pragma('synchronous = NORMAL')
    database.pragma(`journal_size_limit = ${SQLITE_JOURNAL_SIZE_LIMIT_BYTES}`)
  }
  return database
}

/**
 * Starts every transaction with BEGIN IMMEDIATE instead of SQLite's default
 * DEFERRED. A deferred transaction that reads and then writes must upgrade its
 * snapshot to the write lock; if another process committed in between, SQLite
 * fails it with SQLITE_BUSY at once, without consulting busy_timeout. Taking
 * the write lock up front makes contending transactions queue on busy_timeout
 * and see the latest commit, the SQLite counterpart of `forUpdate()`.
 */
class ImmediateSqliteDriver extends SqliteDriver {
  override async beginTransaction(connection: DatabaseConnection) {
    await connection.executeQuery(CompiledQuery.raw('begin immediate'))
  }
}

class ImmediateSqliteDialect extends SqliteDialect {
  readonly #config: SqliteDialectConfig

  constructor(config: SqliteDialectConfig) {
    super(config)
    this.#config = config
  }

  override createDriver() {
    return new ImmediateSqliteDriver(this.#config)
  }
}

/**
 * Runs `run` while holding SQLite's write lock so that only one process
 * migrates. Kysely runs SQLite migrations without a transaction and its SQLite
 * migration lock is a no-op, so two processes starting together would both
 * apply the same migration. A process that waits here sees the finished schema
 * and has nothing left to apply. Also makes the migration atomic.
 */
async function withSqliteWriteLock<T>(db: Kysely<Database>, run: () => Promise<T>) {
  await retrySqliteBusyUntil(Date.now() + SQLITE_STARTUP_LOCK_TIMEOUT_MS, () =>
    sql`begin immediate`.execute(db),
  )
  try {
    const result = await run()
    await sql`commit`.execute(db)
    return result
  } catch (err) {
    await sql`rollback`.execute(db)
    throw err
  }
}

export const getDatabase = createSingletonPromise(async () => {
  if (process.env.NODE_CAGED === 'true' && env.DB_DRIVER === 'sqlite')
    throw new Error('SQLite is not supported with `caged` image variant.')

  const dialect = await match(env)
    .with({ DB_DRIVER: 'postgres' }, async (env) => {
      const pool = new pg.Pool(
        env.DB_POSTGRES_URL
          ? {
              connectionString: env.DB_POSTGRES_URL,
              max: 10,
            }
          : {
              database: env.DB_POSTGRES_DATABASE,
              host: env.DB_POSTGRES_HOST,
              password: env.DB_POSTGRES_PASSWORD,
              port: env.DB_POSTGRES_PORT,
              user: env.DB_POSTGRES_USER,
              max: 10,
            },
      )
      // Fail fast on bad connection settings, then hand the client back: a
      // checked-out client keeps `pool.end()` (Kysely's `destroy()`) waiting forever.
      const probe = await pool.connect()
      probe.release()

      return new PostgresDialect({
        pool,
      })
    })
    .with(
      {
        DB_DRIVER: 'mysql',
      },
      async (env) => {
        const pool = createPool({
          database: env.DB_MYSQL_DATABASE,
          host: env.DB_MYSQL_HOST,
          password: env.DB_MYSQL_PASSWORD,
          port: env.DB_MYSQL_PORT,
          user: env.DB_MYSQL_USER,
          connectionLimit: 10,
        })

        return new MysqlDialect({
          pool,
        })
      },
    )
    .with(
      {
        DB_DRIVER: 'sqlite',
      },
      async (env) =>
        new ImmediateSqliteDialect({
          database: await openSqlite(env.DB_SQLITE_PATH, env.DB_SQLITE_JOURNAL_MODE),
        }),
    )
    .exhaustive()

  const db = new Kysely<Database>({
    dialect,
    log: (event) => {
      if (event.level === 'error')
        dbLogger.error('Query failed', {
          durationMs: event.queryDurationMillis,
          error: event.error,
          sql: event.query.sql,
          params: event.query.parameters,
        })
      else if (event.level === 'query' && env.DEBUG)
        dbLogger.debug('Executed query', {
          durationMs: event.queryDurationMillis,
          sql: event.query.sql,
          params: event.query.parameters,
        })
    },
  })

  logger.info('Migrating database...')
  const hooks = createHooks<{
    afterMigrate: () => Promise<void>
  }>()
  const migrator = new Migrator({
    db,
    provider: {
      async getMigrations() {
        return migrations(env.DB_DRIVER, hooks)
      },
    },
  })
  const migrate = async () => {
    const { error, results } = await migrator.migrateToLatest()
    if (error) throw error
    return results
  }
  let results
  try {
    results = env.DB_DRIVER === 'sqlite' ? await withSqliteWriteLock(db, migrate) : await migrate()
  } catch (err) {
    logger.error('Database migration failed', err)
    // eslint-disable-next-line unicorn/no-process-exit
    process.exit(1)
  }
  await hooks.callHook('afterMigrate')
  logger.debug('Migration results', results)
  logger.success('Database migrated')

  return db
})

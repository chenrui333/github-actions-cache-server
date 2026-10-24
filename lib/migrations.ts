import type { Hookable } from 'hookable'
import type { Kysely } from 'kysely'
import type { Migration } from 'kysely/migration'
import type { Env } from './schemas'
import { setTimeout as sleep } from 'node:timers/promises'
import { sql } from 'kysely'
import { Migrator } from 'kysely/migration'
import { reservationHash } from './helpers'
import { Storage } from './storage'

type MigrationHooks = Hookable<{
  afterMigrate: () => Promise<void>
}>

export function migrations(driver: Env['DB_DRIVER'], hooks: MigrationHooks) {
  // MySQL requires `DROP INDEX ... ON table`, other drivers reject it
  async function dropIndex(db: Kysely<unknown>, index: string, table: string) {
    const query = db.schema.dropIndex(index)
    await (driver === 'mysql' ? query.on(table) : query).execute()
  }

  return {
    $0_init: {
      async up(db) {
        const idType = driver === 'mysql' ? 'varchar(36)' : 'text'
        await db.schema
          .createTable('storage_locations')
          .addColumn('id', idType, (col) => col.primaryKey())
          .addColumn('folderName', 'text', (col) => col.notNull())
          .addColumn('partCount', 'integer', (col) => col.notNull())
          .addColumn('mergeStartedAt', 'bigint')
          .addColumn('mergedAt', 'bigint')
          .addColumn('partsDeletedAt', 'bigint')
          .addColumn('lastDownloadedAt', 'bigint')
          .execute()

        await db.schema
          .createTable('cache_entries')
          .addColumn('id', idType, (col) => col.primaryKey())
          .addColumn('key', driver === 'mysql' ? 'varchar(512)' : 'text', (col) => col.notNull())
          .addColumn('version', driver === 'mysql' ? 'varchar(255)' : 'text', (col) =>
            col.notNull(),
          )
          .addColumn('updatedAt', 'bigint', (col) => col.notNull())
          .addColumn('locationId', idType, (col) =>
            col.notNull().references('storage_locations.id').onDelete('cascade'),
          )
          .execute()

        await db.schema
          .createTable('uploads')
          .addColumn('id', 'bigint', (col) => col.primaryKey())
          .addColumn('key', driver === 'mysql' ? 'varchar(512)' : 'text', (col) => col.notNull())
          .addColumn('version', driver === 'mysql' ? 'varchar(255)' : 'text', (col) =>
            col.notNull(),
          )
          .addColumn('createdAt', 'bigint', (col) => col.notNull())
          .addColumn('lastPartUploadedAt', 'bigint')
          .addColumn('folderName', 'text', (col) => col.notNull())
          .execute()

        await db.schema
          .createIndex('idx_cache_entries_key_version')
          .on('cache_entries')
          .columns(['key', 'version'])
          .execute()

        await db.schema
          .createIndex('idx_uploads_key_version')
          .on('uploads')
          .columns(['key', 'version'])
          .execute()
      },
      async down(db) {
        await db.schema.dropTable('uploads').execute()
        await db.schema.dropTable('cache_entries').execute()
        await db.schema.dropTable('storage_locations').execute()
      },
    },
    $1_upload_part_counts: {
      async up(db) {
        await db.schema
          .alterTable('uploads')
          .addColumn('finishedPartUploadCount', 'integer', (col) => col.notNull().defaultTo(0))
          .execute()
        await db.schema
          .alterTable('uploads')
          .addColumn('startedPartUploadCount', 'integer', (col) => col.notNull().defaultTo(0))
          .execute()
      },
      async down(db) {
        await db.schema.alterTable('uploads').dropColumn('startedPartUploadCount').execute()
        await db.schema.alterTable('uploads').dropColumn('finishedPartUploadCount').execute()
      },
    },
    $2_scopes: {
      async up(db) {
        const scopeColumnType = driver === 'mysql' ? 'varchar(255)' : 'text'

        await Promise.all([
          db.deleteFrom('cache_entries').execute(),
          db.deleteFrom('storage_locations').execute(),
          db.deleteFrom('uploads').execute(),
        ])

        await db.schema
          .alterTable('cache_entries')
          .addColumn('scope', scopeColumnType, (col) => col.notNull())
          .execute()
        await db.schema
          .createIndex('idx_cache_entries_scope')
          .on('cache_entries')
          .columns(['scope'])
          .execute()

        await db.schema
          .alterTable('uploads')
          .addColumn('scope', scopeColumnType, (col) => col.notNull())
          .execute()
        await db.schema.createIndex('idx_uploads_scope').on('uploads').columns(['scope']).execute()
      },
      async down(db) {
        await dropIndex(db, 'idx_cache_entries_scope', 'cache_entries')
        await db.schema.alterTable('cache_entries').dropColumn('scope').execute()
        await dropIndex(db, 'idx_uploads_scope', 'uploads')
        await db.schema.alterTable('uploads').dropColumn('scope').execute()
      },
    },
    $3_repoId: {
      async up(db) {
        const repoIdColumnType = driver === 'mysql' ? 'varchar(255)' : 'text'

        await Promise.all([
          db.deleteFrom('cache_entries').execute(),
          db.deleteFrom('storage_locations').execute(),
          db.deleteFrom('uploads').execute(),
        ])

        hooks.hook('afterMigrate', async () => {
          const adapter = await Storage.getAdapterFromEnv()
          await adapter.clear()
        })

        await db.schema
          .alterTable('cache_entries')
          .addColumn('repoId', repoIdColumnType, (col) => col.notNull())
          .execute()
        await db.schema
          .createIndex('idx_cache_entries_repoId')
          .on('cache_entries')
          .columns(['repoId'])
          .execute()

        await db.schema
          .alterTable('uploads')
          .addColumn('repoId', repoIdColumnType, (col) => col.notNull())
          .execute()
        await db.schema
          .createIndex('idx_uploads_repoId')
          .on('uploads')
          .columns(['repoId'])
          .execute()
      },
      async down(db) {
        await dropIndex(db, 'idx_cache_entries_repoId', 'cache_entries')
        await db.schema.alterTable('cache_entries').dropColumn('repoId').execute()
        await dropIndex(db, 'idx_uploads_repoId', 'uploads')
        await db.schema.alterTable('uploads').dropColumn('repoId').execute()
      },
    },
    $4_storage_leases: {
      async up(db) {
        const idType = driver === 'mysql' ? 'varchar(36)' : 'text'
        const scopeType = driver === 'mysql' ? 'varchar(16)' : 'text'

        await db.schema
          .createTable('merge_leases')
          .addColumn('storageLocationId', idType, (col) =>
            col.primaryKey().references('storage_locations.id').onDelete('cascade'),
          )
          .addColumn('token', idType, (col) => col.notNull())
          .addColumn('expiresAt', 'bigint', (col) => col.notNull())
          .execute()

        await db.schema
          .createTable('storage_reader_leases')
          .addColumn('id', idType, (col) => col.primaryKey())
          .addColumn('storageLocationId', idType, (col) =>
            col.notNull().references('storage_locations.id').onDelete('cascade'),
          )
          .addColumn('scope', scopeType, (col) => col.notNull())
          .addColumn('expiresAt', 'bigint', (col) => col.notNull())
          .execute()

        await db.schema
          .createIndex('idx_storage_reader_leases_location_expiry')
          .on('storage_reader_leases')
          .columns(['storageLocationId', 'expiresAt'])
          .execute()
      },
      async down(db) {
        await db.schema.dropTable('storage_reader_leases').execute()
        await db.schema.dropTable('merge_leases').execute()
      },
    },
    $5_storage_location_sizes: {
      async up(db) {
        await db.schema.alterTable('storage_locations').addColumn('sizeBytes', 'bigint').execute()
      },
      async down(db) {
        await db.schema.alterTable('storage_locations').dropColumn('sizeBytes').execute()
      },
    },
    $6_cache_entries_locationId_index: {
      async up(db) {
        await db.schema
          .createIndex('idx_cache_entries_locationId')
          .on('cache_entries')
          .columns(['locationId'])
          .execute()
      },
      async down(db) {
        await dropIndex(db, 'idx_cache_entries_locationId', 'cache_entries')
      },
    },
    $7_uploads_reservationHash: {
      async up(db) {
        // A unique index over (key, version, scope, repoId) exceeds MySQL's
        // 3072-byte key limit, so it covers a fixed-width hash of the tuple.
        // Nullable so replicas still on the previous version can insert during a rollout.
        await db.schema.alterTable('uploads').addColumn('reservationHash', 'char(64)').execute()

        const uploads = await db
          .selectFrom('uploads')
          .select(['id', 'key', 'version', 'scope', 'repoId'])
          .orderBy('createdAt', 'desc')
          .execute()
        const seen = new Set<string>()
        for (const { id, ...reservation } of uploads) {
          const hash = reservationHash(reservation)
          // keep the newest upload per reservation, cleanup:orphaned-storage reclaims the rest's parts
          if (seen.has(hash)) {
            await db.deleteFrom('uploads').where('id', '=', id).execute()
            continue
          }
          seen.add(hash)
          await db
            .updateTable('uploads')
            .set({ reservationHash: hash })
            .where('id', '=', id)
            .execute()
        }

        await db.schema
          .createIndex('idx_uploads_reservationHash')
          .unique()
          .on('uploads')
          .column('reservationHash')
          .execute()
      },
      async down(db) {
        await dropIndex(db, 'idx_uploads_reservationHash', 'uploads')
        await db.schema.alterTable('uploads').dropColumn('reservationHash').execute()
      },
    },
    $8_exact_key_scope_collation: {
      // MySQL's default collation ignores case, accents and trailing spaces, GitHub compares exactly
      async up(db) {
        if (driver !== 'mysql') return
        for (const table of ['cache_entries', 'uploads'])
          await db.schema
            .alterTable(table)
            .modifyColumn('key', sql`varchar(512) collate utf8mb4_0900_bin`, (col) => col.notNull())
            .modifyColumn('scope', sql`varchar(255) collate utf8mb4_0900_bin`, (col) =>
              col.notNull(),
            )
            .execute()
      },
      async down(db) {
        if (driver !== 'mysql') return
        for (const table of ['cache_entries', 'uploads'])
          await db.schema
            .alterTable(table)
            .modifyColumn('key', 'varchar(512)', (col) => col.notNull())
            .modifyColumn('scope', 'varchar(255)', (col) => col.notNull())
            .execute()
      },
    },
  } satisfies Record<string, Migration>
}

/**
 * Migrations a downstream build released on top of the shared `$0`-`$6`. Their
 * ledger rows are dropped, their schema is kept by `ensureDownstreamSchema`: an
 * executed migration a build doesn't know makes its migrator refuse to start,
 * so a ledger listing only this build's migrations keeps the database usable by
 * any later release.
 */
const DOWNSTREAM_MIGRATIONS = ['$7_cache_entries_lookup_indexes', '$8_upload_capability_hash']

async function forgetDownstreamMigrations(db: Kysely<any>) {
  const tables = await db.introspection.getTables({ withInternalKyselyTables: true })
  if (tables.every((table) => table.name !== 'kysely_migration')) return
  await db.deleteFrom('kysely_migration').where('name', 'in', DOWNSTREAM_MIGRATIONS).execute()
}

async function hasColumn(db: Kysely<any>, table: string, column: string) {
  const tables = await db.introspection.getTables()
  return tables.some((t) => t.name === table && t.columns.some((c) => c.name === column))
}

/** Idempotent: runs on every start, after the migrations. */
async function ensureDownstreamSchema(db: Kysely<any>, driver: Env['DB_DRIVER']) {
  // SHA-256 of the capability carried by an Upload's URL; nullable, Uploads
  // created without one have none.
  if (!(await hasColumn(db, 'uploads', 'capabilityHash'))) {
    try {
      await db.schema.alterTable('uploads').addColumn('capabilityHash', 'varchar(64)').execute()
    } catch (err) {
      // another replica added it first
      if (!(await hasColumn(db, 'uploads', 'capabilityHash'))) throw err
    }
  }

  // Every `matchCacheEntry` probe filters on repoId, scope and version and then
  // on the key; with `case_sensitive_like = on` this binary index also serves
  // the prefix matches. SQLite only: MySQL's four utf8mb4 varchar columns
  // exceed InnoDB's 3072-byte index key limit, and Postgres limits a B-tree
  // entry to about a third of a page, which four unbounded text columns of an
  // existing row could exceed.
  if (driver !== 'sqlite') return
  await db.schema
    .createIndex('idx_cache_entries_lookup')
    .ifNotExists()
    .on('cache_entries')
    .columns(['repoId', 'scope', 'version', 'key'])
    .execute()
  // served prefix matches while `like` ignored case
  await db.schema.dropIndex('idx_cache_entries_lookup_prefix').ifExists().execute()
}

/** Upper bound for waiting on another process that is migrating the SQLite file. */
const SQLITE_STARTUP_LOCK_TIMEOUT_MS = 120_000

/**
 * Runs `run` while holding SQLite's write lock so that only one process
 * migrates: the `node-cluster` primary and its workers all migrate on start,
 * and Kysely's SQLite migration lock is a no-op. A process that waits here
 * sees the finished schema and has nothing left to apply. SQLite DDL is
 * transactional, so a failed or interrupted migration leaves no partial schema.
 */
async function withSqliteWriteLock<T>(db: Kysely<any>, run: () => Promise<T>) {
  const deadline = Date.now() + SQLITE_STARTUP_LOCK_TIMEOUT_MS
  for (;;) {
    try {
      await sql`begin immediate`.execute(db)
      break
    } catch (err) {
      // better-sqlite3 reports extended codes: SQLITE_BUSY, SQLITE_BUSY_SNAPSHOT, ...
      const code = (err as { code?: unknown } | null)?.code
      if (typeof code !== 'string' || !code.startsWith('SQLITE_BUSY') || Date.now() >= deadline)
        throw err
      await sleep(100)
    }
  }
  try {
    const result = await run()
    await sql`commit`.execute(db)
    return result
  } catch (err) {
    await sql`rollback`.execute(db)
    throw err
  }
}

export async function migrateToLatest(
  db: Kysely<any>,
  driver: Env['DB_DRIVER'],
  hooks: MigrationHooks,
) {
  const migrator = new Migrator({
    db,
    provider: {
      async getMigrations() {
        return migrations(driver, hooks)
      },
    },
  })
  const migrate = async () => {
    await forgetDownstreamMigrations(db)
    const { error, results } = await migrator.migrateToLatest()
    if (error) throw error
    await ensureDownstreamSchema(db, driver)
    return results
  }
  return driver === 'sqlite' ? withSqliteWriteLock(db, migrate) : migrate()
}

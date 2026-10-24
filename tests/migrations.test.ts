import type { Migration } from 'kysely/migration'
import { createHash, randomBytes } from 'node:crypto'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'

import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import { Kysely, MysqlDialect, PostgresDialect, sql, SqliteDialect } from 'kysely'
import { Migrator } from 'kysely/migration'
import { createPool } from 'mysql2'
import pg from 'pg'
import { match } from 'ts-pattern'
import { afterAll, describe, expect, test } from 'vitest'
import { env } from '~/lib/env'
import { uploadCapabilityMatches } from '~/lib/helpers'
import { migrateToLatest, migrations } from '~/lib/migrations'
import { TEST_TEMP_DIR } from './setup'

// never fired: `$3_repoId` would clear the shared test storage
const hooks = createHooks<{ afterMigrate: () => Promise<void> }>()
const upstream = migrations(env.DB_DRIVER, hooks)
// code unit order, as Kysely sorts migration names
const byName = (a: string, b: string) => (a < b ? -1 : Number(a > b))
const UPSTREAM_LEDGER = Object.keys(upstream).toSorted(byName)

/**
 * The migrations of a downstream build that released two migrations of its own
 * on top of `$0`-`$6`, as it released them.
 */
const downstream: Record<string, Migration> = {
  ...Object.fromEntries(Object.entries(upstream).filter(([name]) => name < '$7')),
  $7_cache_entries_lookup_indexes: {
    async up(db) {
      if (env.DB_DRIVER !== 'sqlite') return
      await db.schema
        .createIndex('idx_cache_entries_lookup')
        .on('cache_entries')
        .columns(['repoId', 'scope', 'version', 'key'])
        .execute()
      await sql`create index ${sql.ref('idx_cache_entries_lookup_prefix')} on ${sql.table('cache_entries')} (${sql.ref('repoId')}, ${sql.ref('scope')}, ${sql.ref('version')}, ${sql.ref('key')} collate nocase)`.execute(
        db,
      )
    },
  },
  $8_upload_capability_hash: {
    async up(db) {
      await db.schema.alterTable('uploads').addColumn('capabilityHash', 'varchar(64)').execute()
    },
  },
}
const DOWNSTREAM_LEDGER = Object.keys(downstream).toSorted(byName)

/** Reverts a database migrated by this build to the downstream release (SQLite). */
const ROLLBACK_TO_DOWNSTREAM_SQL = `
begin immediate;
delete from uploads
where capabilityHash is null
  and createdAt >= (select cast(strftime('%s', timestamp) as integer) * 1000
                    from kysely_migration where name = '$7_uploads_reservationHash');
drop index "idx_uploads_reservationHash";
alter table "uploads" drop column "reservationHash";
create index if not exists "idx_cache_entries_lookup" on "cache_entries" ("repoId", "scope", "version", "key");
create index if not exists "idx_cache_entries_lookup_prefix" on "cache_entries" ("repoId", "scope", "version", "key" collate nocase);
delete from kysely_migration where name in ('$7_uploads_reservationHash', '$8_exact_key_scope_collation');
insert into kysely_migration (name, timestamp) values
  ('$7_cache_entries_lookup_indexes', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  ('$8_upload_capability_hash', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
commit;
`

const databases: Kysely<any>[] = []

afterAll(async () => {
  for (const db of databases) await db.destroy()
})

function sqliteFile(name: string) {
  return path.join(TEST_TEMP_DIR, `migrations_${name}.sqlite`)
}

async function createDatabase(name: string) {
  const dbName = `migrations_${name}_${Date.now()}`
  const db = await match(env)
    .with({ DB_DRIVER: 'sqlite' }, async () => {
      const database = new SQLite(sqliteFile(dbName))
      return new Kysely<any>({ dialect: new SqliteDialect({ database }) })
    })
    .with({ DB_DRIVER: 'postgres' }, async (env) => {
      const connection = {
        host: env.DB_POSTGRES_HOST,
        port: env.DB_POSTGRES_PORT,
        user: env.DB_POSTGRES_USER,
        password: env.DB_POSTGRES_PASSWORD,
      }
      // dropped with the test container
      const admin = new pg.Client({ ...connection, database: env.DB_POSTGRES_DATABASE })
      await admin.connect()
      await admin.query(`create database "${dbName}"`)
      await admin.end()
      return new Kysely<any>({
        dialect: new PostgresDialect({ pool: new pg.Pool({ ...connection, database: dbName }) }),
      })
    })
    .with({ DB_DRIVER: 'mysql' }, async (env) => {
      const connection = {
        host: env.DB_MYSQL_HOST,
        port: env.DB_MYSQL_PORT,
        user: env.DB_MYSQL_USER,
        password: env.DB_MYSQL_PASSWORD,
      }
      const admin = createPool(connection).promise()
      await admin.query(`create database \`${dbName}\``)
      await admin.end()
      return new Kysely<any>({
        dialect: new MysqlDialect({ pool: createPool({ ...connection, database: dbName }) }),
      })
    })
    .exhaustive()
  databases.push(db)
  return db
}

/** Kysely's default migrator, as the downstream release and upstream releases run it. */
async function migrateStrictly(db: Kysely<any>, list: Record<string, Migration>) {
  return new Migrator({
    db,
    provider: {
      async getMigrations() {
        return list
      },
    },
  }).migrateToLatest()
}

async function ledger(db: Kysely<any>) {
  const rows = await db.selectFrom('kysely_migration').select('name').execute()
  return rows.map((row) => row.name as string).toSorted(byName)
}

/** Columns and indexes of the application tables, independent of column order. */
async function schema(db: Kysely<any>) {
  return match(env.DB_DRIVER)
    .with('sqlite', async () => {
      const tables = await sql<{ name: string }>`
        select name from sqlite_master
        where type = 'table' and name not like 'sqlite_%' and name not like 'kysely_%'
        order by name`.execute(db)
      const columns = []
      for (const { name } of tables.rows) {
        const info = await sql<Record<string, unknown>>`
          select name, type, "notnull", dflt_value, pk from pragma_table_info(${name})
          order by name`.execute(db)
        columns.push({ table: name, columns: info.rows })
      }
      const indexes = await sql`
        select name, tbl_name, sql from sqlite_master
        where type = 'index' and name not like 'sqlite_%'
        order by name`.execute(db)
      return { columns, indexes: indexes.rows }
    })
    .with('postgres', async () => {
      const columns = await sql`
        select table_name, column_name, data_type, character_maximum_length, is_nullable, column_default
        from information_schema.columns
        where table_schema = 'public' and table_name not like 'kysely_%'
        order by table_name, column_name`.execute(db)
      const indexes = await sql`
        select tablename, indexname, indexdef from pg_indexes
        where schemaname = 'public' and tablename not like 'kysely_%'
        order by indexname`.execute(db)
      return { columns: columns.rows, indexes: indexes.rows }
    })
    .with('mysql', async () => {
      const columns = await sql`
        select table_name, column_name, column_type, is_nullable, column_default, collation_name
        from information_schema.columns
        where table_schema = database() and table_name not like 'kysely_%'
        order by table_name, column_name`.execute(db)
      const indexes = await sql`
        select table_name, index_name, seq_in_index, column_name, non_unique
        from information_schema.statistics
        where table_schema = database() and table_name not like 'kysely_%'
        order by table_name, index_name, seq_in_index`.execute(db)
      return { columns: columns.rows, indexes: indexes.rows }
    })
    .exhaustive()
}

const reservation = { key: 'key', version: 'version', scope: 'refs/heads/main', repoId: '1' }
const idleUpload = {
  lastPartUploadedAt: null,
  finishedPartUploadCount: 0,
  startedPartUploadCount: 0,
}
// The capability the downstream release issued for the surviving Upload, and
// the hash it stored for it (SHA-256, hex), as it computed them.
const downstreamCapability = randomBytes(32).toString('hex')
const downstreamCapabilityHash = createHash('sha256').update(downstreamCapability).digest('hex')

/** A database migrated and used by the downstream release. */
async function createDownstreamDatabase(name: string) {
  const db = await createDatabase(name)
  const { error } = await migrateStrictly(db, downstream)
  expect(error).toBeUndefined()

  await db
    .insertInto('storage_locations')
    .values({ id: 'location', folderName: 'location', partCount: 1, sizeBytes: 3 })
    .execute()
  await db
    .insertInto('cache_entries')
    .values({ id: 'entry', ...reservation, updatedAt: 1, locationId: 'location' })
    .execute()
  // two Uploads holding one reservation (a lost check-then-insert race) and an unrelated one
  await db
    .insertInto('uploads')
    .values([
      {
        ...idleUpload,
        ...reservation,
        id: 1,
        folderName: '1',
        createdAt: 1,
        capabilityHash: 'a'.repeat(64),
      },
      {
        ...idleUpload,
        ...reservation,
        id: 2,
        folderName: '2',
        createdAt: 2,
        capabilityHash: downstreamCapabilityHash,
      },
      {
        ...idleUpload,
        ...reservation,
        key: 'other',
        id: 3,
        folderName: '3',
        createdAt: 3,
        capabilityHash: null,
      },
    ])
    .execute()
  // keeps the upgrade's ledger timestamps after these, as separate deploys would
  await sleep(10)
  return db
}

describe('migrations', () => {
  test('a fresh database migrates to an upstream-only ledger', async () => {
    const db = await createDatabase('fresh')

    const results = await migrateToLatest(db, env.DB_DRIVER, hooks)

    expect(results?.map((r) => [r.migrationName, r.status])).toEqual(
      UPSTREAM_LEDGER.map((name) => [name, 'Success']),
    )
    expect(await ledger(db)).toEqual(UPSTREAM_LEDGER)
    const { error, results: strict } = await migrateStrictly(db, upstream)
    expect(error).toBeUndefined()
    expect(strict).toEqual([])
  })

  test('a downstream database runs the upstream migrations and matches a fresh one', async () => {
    const db = await createDownstreamDatabase('downstream')
    expect(await ledger(db)).toEqual(DOWNSTREAM_LEDGER)

    const results = await migrateToLatest(db, env.DB_DRIVER, hooks)

    expect(results?.map((r) => [r.migrationName, r.status])).toEqual([
      ['$7_uploads_reservationHash', 'Success'],
      ['$8_exact_key_scope_collation', 'Success'],
    ])
    expect(await ledger(db)).toEqual(UPSTREAM_LEDGER)
    // the newest Upload keeps the reservation, with its capability hash intact
    const uploads = await db
      .selectFrom('uploads')
      .select(['id', 'capabilityHash', 'reservationHash'])
      .orderBy('id')
      .execute()
    expect(uploads.map((u) => [u.id, u.capabilityHash])).toEqual([
      [2, downstreamCapabilityHash],
      [3, null],
    ])
    // so the Upload URL the downstream release issued still authorizes its parts
    expect(uploadCapabilityMatches(uploads[0]!.capabilityHash, downstreamCapability)).toBe(true)
    expect(
      uploadCapabilityMatches(uploads[0]!.capabilityHash, randomBytes(32).toString('hex')),
    ).toBe(false)
    expect(uploads.every((u) => u.reservationHash?.length === 64)).toBe(true)
    expect(await db.selectFrom('cache_entries').selectAll().execute()).toEqual([
      { id: 'entry', ...reservation, updatedAt: 1, locationId: 'location' },
    ])

    const fresh = await createDatabase('reference')
    await migrateToLatest(fresh, env.DB_DRIVER, hooks)
    expect(await schema(db)).toEqual(await schema(fresh))

    // an upstream release starts on it, and this build starts on it again
    const { error, results: strict } = await migrateStrictly(db, upstream)
    expect(error).toBeUndefined()
    expect(strict).toEqual([])
    expect(await migrateToLatest(db, env.DB_DRIVER, hooks)).toEqual([])
    expect(await schema(db)).toEqual(await schema(fresh))
  })

  test.runIf(env.DB_DRIVER === 'sqlite')(
    'the rollback returns a migrated database to the downstream release',
    async () => {
      const db = await createDownstreamDatabase('rollback')
      const downstreamSchema = await schema(db)
      await migrateToLatest(db, env.DB_DRIVER, hooks)
      await db
        .insertInto('uploads')
        .values({
          ...idleUpload,
          ...reservation,
          key: 'new',
          id: 4,
          folderName: '4',
          createdAt: Date.now(),
        })
        .execute()

      for (const statement of ROLLBACK_TO_DOWNSTREAM_SQL.split(';'))
        if (statement.trim()) await sql.raw(statement).execute(db)

      expect(await ledger(db)).toEqual(DOWNSTREAM_LEDGER)
      expect(await schema(db)).toEqual(downstreamSchema)
      // the Upload this build created has no capability, the downstream release could not use it
      const uploads = await db.selectFrom('uploads').select('id').orderBy('id').execute()
      expect(uploads.map((u) => u.id)).toEqual([2, 3])
      const { error, results } = await migrateStrictly(db, downstream)
      expect(error).toBeUndefined()
      expect(results).toEqual([])

      await sleep(10)
      await migrateToLatest(db, env.DB_DRIVER, hooks)
      expect(await ledger(db)).toEqual(UPSTREAM_LEDGER)
    },
  )

  test.runIf(env.DB_DRIVER === 'sqlite')('processes starting together migrate once', async () => {
    const file = sqliteFile(`concurrent_${Date.now()}`)
    const seed = await createDownstreamDatabase('concurrent')
    await sql`vacuum into ${file}`.execute(seed)
    // no busy wait inside better-sqlite3: it would block this single-threaded test
    const [a, b] = [0, 1].map(() => {
      const db = new Kysely<any>({
        dialect: new SqliteDialect({ database: new SQLite(file, { timeout: 0 }) }),
      })
      databases.push(db)
      return db
    })

    const results = await Promise.all([
      migrateToLatest(a, env.DB_DRIVER, hooks),
      migrateToLatest(b, env.DB_DRIVER, hooks),
    ])

    expect(results.map((r) => r?.length ?? -1).toSorted((x, y) => x - y)).toEqual([0, 2])
    expect(await ledger(a)).toEqual(UPSTREAM_LEDGER)
  })

  test('a database with executed migrations unknown to this build is refused', async () => {
    const db = await createDatabase('unknown')
    await migrateToLatest(db, env.DB_DRIVER, hooks)
    await db
      .insertInto('kysely_migration')
      .values({ name: '$9_from_the_future', timestamp: new Date().toISOString() })
      .execute()

    await expect(migrateToLatest(db, env.DB_DRIVER, hooks)).rejects.toThrow(
      'previously executed migration $9_from_the_future is missing',
    )
  })
})

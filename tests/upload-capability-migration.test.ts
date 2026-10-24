import path from 'node:path'

import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect } from 'kysely'
import { Migrator } from 'kysely/migration'
import { createPool } from 'mysql2'
import pg from 'pg'
import { match } from 'ts-pattern'
import { afterAll, describe, expect, test } from 'vitest'
import { env } from '~/lib/env'
import { hashUploadCapability, reservationHash } from '~/lib/helpers'
import { migrations } from '~/lib/migrations'
import { TEST_TEMP_DIR } from './setup'

// never fired: `$3_repoId` would clear the shared test storage
const hooks = createHooks<{ afterMigrate: () => Promise<void> }>()

const databases: Kysely<any>[] = []

afterAll(async () => {
  for (const db of databases) await db.destroy()
})

/** An empty database of the configured driver, apart from the test server's. */
async function createDatabase(name: string) {
  const dbName = `capability_migration_${name}_${Date.now()}`
  const db = await match(env)
    .with({ DB_DRIVER: 'sqlite' }, async () => {
      const database = new SQLite(path.join(TEST_TEMP_DIR, `${dbName}.sqlite`))
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

function migrator(db: Kysely<any>) {
  return new Migrator({
    db,
    provider: {
      async getMigrations() {
        return migrations(env.DB_DRIVER, hooks)
      },
    },
  })
}

async function capabilityColumns(db: Kysely<any>) {
  const tables = await db.introspection.getTables()
  return (
    tables
      .find((table) => table.name === 'uploads')
      ?.columns.filter((column) => column.name === 'capabilityHash') ?? []
  )
}

const reservation = { key: 'key', version: 'v1', scope: 'refs/heads/main', repoId: '1' }
const capabilityHash = hashUploadCapability('capability')

async function insertUpload(db: Kysely<any>, values: Record<string, unknown> = {}) {
  await db
    .insertInto('uploads')
    .values({
      ...reservation,
      id: 1,
      folderName: '1',
      createdAt: 1,
      lastPartUploadedAt: null,
      finishedPartUploadCount: 0,
      startedPartUploadCount: 0,
      reservationHash: reservationHash(reservation),
      ...values,
    })
    .execute()
}

describe('$9_upload_capability_hash', () => {
  test('adds the capability column, and migrates down and up', async () => {
    const db = await createDatabase('fresh')

    const { error, results } = await migrator(db).migrateToLatest()
    expect(error).toBeUndefined()
    expect(results?.at(-1)).toMatchObject({
      migrationName: '$9_upload_capability_hash',
      status: 'Success',
    })
    expect(await capabilityColumns(db)).toMatchObject([{ isNullable: true }])
    await insertUpload(db, { capabilityHash })

    const down = await migrator(db).migrateDown()
    expect(down.error).toBeUndefined()
    expect(await capabilityColumns(db)).toEqual([])

    const up = await migrator(db).migrateUp()
    expect(up.error).toBeUndefined()
    expect(await capabilityColumns(db)).toHaveLength(1)
  })

  test('keeps a capability column that already exists, with its hashes', async () => {
    const db = await createDatabase('existing')
    const before = await migrator(db).migrateTo('$8_exact_key_scope_collation')
    expect(before.error).toBeUndefined()
    // as a build that added the column before this migration existed left it
    await db.schema.alterTable('uploads').addColumn('capabilityHash', 'varchar(64)').execute()
    await insertUpload(db, { capabilityHash })

    const { error, results } = await migrator(db).migrateToLatest()
    expect(error).toBeUndefined()
    expect(results).toEqual([
      { migrationName: '$9_upload_capability_hash', direction: 'Up', status: 'Success' },
    ])
    expect(await capabilityColumns(db)).toHaveLength(1)
    const uploads = await db.selectFrom('uploads').select(['id', 'capabilityHash']).execute()
    expect(uploads.map((upload) => [Number(upload.id), upload.capabilityHash])).toEqual([
      [1, capabilityHash],
    ])
  })
})

import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import { Kysely, SqliteDialect } from 'kysely'
import { Migrator } from 'kysely/migration'
import { expect, test } from 'vitest'
import { migrations } from '~/lib/migrations'

const MIGRATION = '$8_upload_capability_hash'

test('the upload capability migration adds and removes its column', async () => {
  const db = new Kysely<unknown>({
    dialect: new SqliteDialect({ database: new SQLite(':memory:') }),
  })
  const all = migrations('sqlite', createHooks())
  const migrator = new Migrator({
    db,
    provider: {
      async getMigrations() {
        return all
      },
    },
  })
  // Kysely orders migrations by name.
  const names = Object.keys(all).toSorted((a, b) => a.localeCompare(b))
  const previous = names[names.indexOf(MIGRATION) - 1]!

  async function uploadColumns() {
    const tables = await db.introspection.getTables()
    return tables.find((table) => table.name === 'uploads')?.columns.map((column) => column.name)
  }

  try {
    const latest = await migrator.migrateToLatest()
    expect(latest.error).toBeUndefined()
    expect(await uploadColumns()).toContain('capabilityHash')

    const down = await migrator.migrateTo(previous)
    expect(down.error).toBeUndefined()
    expect(down.results?.map((result) => result.migrationName)).toContain(MIGRATION)
    expect(await uploadColumns()).not.toContain('capabilityHash')

    const up = await migrator.migrateTo(MIGRATION)
    expect(up.error).toBeUndefined()
    expect(await uploadColumns()).toContain('capabilityHash')
  } finally {
    await db.destroy()
  }
})

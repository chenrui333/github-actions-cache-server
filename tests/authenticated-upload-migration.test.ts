import type { Database } from '~/lib/db'
import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import { Kysely, SqliteDialect } from 'kysely'
import { Migrator } from 'kysely/migration'
import { expect, test } from 'vitest'
import { migrations } from '~/lib/migrations'

test('completion migration preserves legacy data, adds empty generation metadata and refuses unsafe downgrade', async () => {
  const sqlite = new SQLite(':memory:')
  const db = new Kysely<Database>({
    dialect: new SqliteDialect({ database: sqlite }),
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
  try {
    const baseline = await migrator.migrateTo('$8_upload_capability_hash')
    expect(baseline.error).toBeUndefined()
    await db
      .insertInto('uploads')
      .values({
        id: 42,
        key: 'legacy',
        version: 'v1',
        scope: 'refs/heads/main',
        repoId: 'repo',
        createdAt: 1,
        lastPartUploadedAt: 2,
        folderName: '42',
        startedPartUploadCount: 1,
        finishedPartUploadCount: 1,
        capabilityHash: null,
      })
      .execute()
    await db
      .insertInto('storage_locations')
      .values({
        id: 'old-location',
        folderName: 'old-payload',
        partCount: 1,
        mergedAt: null,
        mergeStartedAt: null,
        partsDeletedAt: null,
        lastDownloadedAt: null,
        sizeBytes: 3,
      })
      .execute()
    await db
      .insertInto('cache_entries')
      .values({
        id: 'old-cache',
        key: 'complete',
        version: 'v1',
        scope: 'refs/heads/main',
        repoId: 'repo',
        updatedAt: 1,
        locationId: 'old-location',
      })
      .execute()
    const upload = await db.selectFrom('uploads').selectAll().executeTakeFirstOrThrow()
    const upgraded = await migrator.migrateToLatest()
    expect(upgraded.error).toBeUndefined()
    expect(await db.selectFrom('uploads').selectAll().executeTakeFirst()).toEqual(upload)
    const preserved = await db.selectFrom('storage_locations').selectAll().executeTakeFirstOrThrow()
    expect(preserved.partNamesJson).toBeNull()
    expect(await db.selectFrom('cache_entries').select('id').execute()).toEqual([
      { id: 'old-cache' },
    ])
    expect(await db.selectFrom('upload_parts').selectAll().execute()).toHaveLength(0)
    expect(await db.selectFrom('upload_receipts').selectAll().execute()).toHaveLength(0)
    expect(sqlite.pragma('index_list(upload_receipts)')).toEqual(
      expect.arrayContaining([expect.objectContaining({ name: 'idx_upload_receipts_location' })]),
    )
    const down = await migrator.migrateTo('$8_upload_capability_hash')
    expect(String(down.error)).toContain('forward-only')
    const tables = await db.introspection.getTables()
    expect(
      tables.find(({ name }) => name === 'storage_locations')?.columns.map(({ name }) => name),
    ).toContain('partNamesJson')
    const oldProvider = new Migrator({
      db,
      provider: {
        async getMigrations() {
          const { $9_authenticated_upload_completion: _, ...old } = all
          return old
        },
      },
    })
    const oldBinary = await oldProvider.migrateToLatest()
    expect(oldBinary.error).toBeDefined()
    expect(await db.selectFrom('uploads').selectAll().executeTakeFirst()).toEqual(upload)
  } finally {
    await db.destroy()
  }
})

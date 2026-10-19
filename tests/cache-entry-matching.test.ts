import { randomUUID } from 'node:crypto'
import path from 'node:path'
import SQLite from 'better-sqlite3'
import { createHooks } from 'hookable'
import { CompiledQuery, Kysely, SqliteDialect } from 'kysely'
import { Migrator } from 'kysely/migration'
import { afterAll, describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import { migrations } from '~/lib/migrations'
import { cacheEntryCandidateQuery, getStorage } from '~/lib/storage'
import { TEST_TEMP_DIR } from './setup'

const driver = process.env.VITEST_DB_DRIVER ?? 'sqlite'
const VERSION = 'v1'
const repoIds: string[] = []

async function seed(
  entries: { key: string; scope?: string; version?: string; updatedAt: number }[],
) {
  const db = await getDatabase()
  const repoId = randomUUID()
  repoIds.push(repoId)
  for (const entry of entries) {
    const locationId = randomUUID()
    await db
      .insertInto('storage_locations')
      .values({
        id: locationId,
        folderName: locationId,
        partCount: 1,
        mergeStartedAt: null,
        mergedAt: null,
        partsDeletedAt: null,
        lastDownloadedAt: null,
        sizeBytes: 0,
      })
      .execute()
    await db
      .insertInto('cache_entries')
      .values({
        id: randomUUID(),
        key: entry.key,
        version: entry.version ?? VERSION,
        scope: entry.scope ?? 'refs/heads/main',
        repoId,
        updatedAt: entry.updatedAt,
        locationId,
      })
      .execute()
  }
  return repoId
}

async function match(repoId: string, keys: [string, ...string[]], scopes = ['refs/heads/main']) {
  const storage = await getStorage()
  const result = await storage.matchCacheEntry({ keys, version: VERSION, scopes, repoId })
  return result && { key: result.match.key, scope: result.match.scope, type: result.type }
}

describe('cache entry matching', () => {
  afterAll(async () => {
    const db = await getDatabase()
    const locations = db
      .selectFrom('cache_entries')
      .select('locationId')
      .where('repoId', 'in', repoIds)
    await db.deleteFrom('storage_locations').where('id', 'in', locations).execute()
    await db.deleteFrom('cache_entries').where('repoId', 'in', repoIds).execute()
  })

  test('an exact primary key wins over newer prefix matches', async () => {
    const repoId = await seed([
      { key: 'npm-abc', updatedAt: 1 },
      { key: 'npm-abc-newer', updatedAt: 2 },
    ])
    expect(await match(repoId, ['npm-abc'])).toMatchObject({
      key: 'npm-abc',
      type: 'exact-primary',
    })
  })

  test('a prefix match returns the most recently updated entry', async () => {
    const repoId = await seed([
      { key: 'npm-1', updatedAt: 1 },
      { key: 'npm-2', updatedAt: 3 },
      { key: 'npm-3', updatedAt: 2 },
      { key: 'other-9', updatedAt: 4 },
    ])
    expect(await match(repoId, ['npm-'])).toMatchObject({ key: 'npm-2', type: 'prefixed-primary' })
  })

  test('restore keys are tried in order, then scopes in order', async () => {
    const repoId = await seed([
      { key: 'yarn-1', updatedAt: 1 },
      { key: 'npm-1', updatedAt: 5 },
      { key: 'npm-feature', scope: 'refs/heads/feature', updatedAt: 0 },
    ])
    expect(await match(repoId, ['miss', 'yarn-', 'npm-'])).toMatchObject({
      key: 'yarn-1',
      type: 'prefixed-restore',
    })
    expect(await match(repoId, ['miss', 'npm-1'])).toMatchObject({
      key: 'npm-1',
      type: 'exact-restore',
    })
    // An older entry in the first scope beats a newer one in a later scope.
    expect(await match(repoId, ['npm-'], ['refs/heads/feature', 'refs/heads/main'])).toMatchObject({
      key: 'npm-feature',
      scope: 'refs/heads/feature',
    })
    expect(await match(repoId, ['npm-'], ['refs/pull/1/merge', 'refs/heads/main'])).toMatchObject({
      key: 'npm-1',
      scope: 'refs/heads/main',
    })
  })

  test('version and repository isolate entries', async () => {
    const repoId = await seed([{ key: 'cargo-1', version: 'v2', updatedAt: 1 }])
    const otherRepo = await seed([{ key: 'cargo-1', updatedAt: 1 }])
    expect(await match(repoId, ['cargo-1'])).toBeUndefined()
    expect(await match(otherRepo, ['cargo-'])).toMatchObject({ key: 'cargo-1' })
  })

  test('like metacharacters in keys match literally', async () => {
    const repoId = await seed([
      { key: 'a%b-1', updatedAt: 1 },
      { key: 'axb-1', updatedAt: 2 },
      { key: 'a_c-1', updatedAt: 3 },
      { key: 'abc-1', updatedAt: 4 },
      { key: String.raw`a\d-1`, updatedAt: 5 },
      { key: 'aed-1', updatedAt: 6 },
      // The escape character itself, and a backslash before a metacharacter.
      { key: 'a!e-1', updatedAt: 7 },
      { key: 'a!%g-1', updatedAt: 8 },
      { key: 'a!xg-1', updatedAt: 9 },
      { key: String.raw`a\%h-1`, updatedAt: 10 },
      { key: String.raw`a\xh-1`, updatedAt: 11 },
    ])
    expect(await match(repoId, ['a%'])).toMatchObject({ key: 'a%b-1' })
    expect(await match(repoId, ['a_'])).toMatchObject({ key: 'a_c-1' })
    expect(await match(repoId, [String.raw`a\d`])).toMatchObject({ key: String.raw`a\d-1` })
    expect(await match(repoId, ['a!e'])).toMatchObject({ key: 'a!e-1' })
    expect(await match(repoId, ['a!%'])).toMatchObject({ key: 'a!%g-1' })
    expect(await match(repoId, [String.raw`a\%`])).toMatchObject({ key: String.raw`a\%h-1` })
  })

  test('prefix matching keeps the driver LIKE case rules', async () => {
    const repoId = await seed([{ key: 'Linux-node-1', updatedAt: 1 }])
    // SQLite and MySQL (default collation) compare LIKE case-insensitively, Postgres does not.
    expect(Boolean(await match(repoId, ['linux-node-']))).toBe(driver !== 'postgres')
  })

  test.runIf(driver === 'sqlite')('sqlite probes use the lookup indexes', async () => {
    const db = await getDatabase()
    const plan = async (matchKind: 'exact' | 'prefix') => {
      const { sql, parameters } = cacheEntryCandidateQuery(db, {
        key: 'npm-',
        match: matchKind,
        version: VERSION,
        scope: 'refs/heads/main',
        repoId: 'r',
      }).compile()
      const { rows } = await db.executeQuery<{ detail: string }>(
        CompiledQuery.raw(`explain query plan ${sql}`, [...parameters]),
      )
      return rows.map((row) => row.detail).join(' | ')
    }
    expect(await plan('exact')).toContain(
      'idx_cache_entries_lookup (repoId=? AND scope=? AND version=? AND key=?)',
    )
    expect(await plan('prefix')).toContain(
      'idx_cache_entries_lookup_prefix (repoId=? AND scope=? AND version=? AND key>? AND key<?)',
    )
  })

  test.runIf(driver === 'sqlite')('the lookup index migration migrates down and up', async () => {
    const file = path.resolve(TEST_TEMP_DIR, `lookup-migration-${randomUUID()}.sqlite`)
    const sqlite = new SQLite(file)
    const db = new Kysely<unknown>({ dialect: new SqliteDialect({ database: sqlite }) })
    const migrator = new Migrator({
      db,
      provider: { getMigrations: async () => migrations('sqlite', createHooks()) },
    })
    const indexes = () =>
      (
        sqlite
          .prepare(
            "select name from sqlite_master where type = 'index' and name like 'idx_cache_entries_lookup%' order by name",
          )
          .all() as { name: string }[]
      ).map((row) => row.name)
    try {
      await expect(migrator.migrateToLatest()).resolves.not.toHaveProperty('error')
      expect(indexes()).toEqual(['idx_cache_entries_lookup', 'idx_cache_entries_lookup_prefix'])
      await expect(migrator.migrateDown()).resolves.not.toHaveProperty('error')
      expect(indexes()).toEqual([])
      await expect(migrator.migrateUp()).resolves.not.toHaveProperty('error')
      expect(indexes()).toEqual(['idx_cache_entries_lookup', 'idx_cache_entries_lookup_prefix'])
    } finally {
      await db.destroy()
    }
  })
})

import fs from 'node:fs/promises'
import path from 'node:path'
import SQLite from 'better-sqlite3'
import { execa } from 'execa'
import { describe, expect, test } from 'vitest'
import { TEST_TEMP_DIR } from './setup'

// Several processes share one SQLite file, as the workers of a `node-cluster`
// deployment do (NITRO_CLUSTER_WORKERS > 1) with DB_SQLITE_JOURNAL_MODE=wal.
// See ADR-0010.
const ITERATIONS = 150

interface ContenderResult {
  pragmas: { journalMode: string; synchronous: number; busyTimeout: number }
  leasesAcquired: number
  transactions: number
  readerLeases: number
  locationDeletes: number
  uploads: number
  violations: string[]
  errors: string[]
}

async function runContenders(
  dir: string,
  count: number,
  iterations: number,
  journalMode?: 'wal' | 'delete',
) {
  await fs.rm(dir, { recursive: true, force: true })
  await fs.mkdir(dir, { recursive: true })
  const dbPath = path.join(dir, 'shared.sqlite')
  // Fresh file: every contender also races the journal mode switch and the migrations.
  const runs = await Promise.all(
    Array.from({ length: count }, () =>
      execa('tsx', ['tests/sqlite-contender.ts'], {
        preferLocal: true,
        env: {
          DB_DRIVER: 'sqlite',
          DB_SQLITE_PATH: dbPath,
          ...(journalMode && { DB_SQLITE_JOURNAL_MODE: journalMode }),
          STORAGE_DRIVER: 'filesystem',
          STORAGE_FILESYSTEM_PATH: path.join(dir, 'storage'),
          API_BASE_URL: 'http://localhost:3000',
          CONTENDER_ITERATIONS: String(iterations),
        },
      }),
    ),
  )
  const results = runs.map((run) => {
    const line = run.stdout.split('\n').find((l) => l.startsWith('RESULT '))
    expect(line, run.stderr).toBeDefined()
    return JSON.parse(line!.slice('RESULT '.length)) as ContenderResult
  })
  for (const result of results) {
    expect(result.errors).toEqual([])
    expect(result.violations).toEqual([])
    expect(result.transactions).toBe(iterations)
    expect(result.uploads).toBe(iterations)
  }
  return { dbPath, results }
}

describe.runIf((process.env.VITEST_DB_DRIVER ?? 'sqlite') === 'sqlite')(
  'sqlite shared by several processes',
  () => {
    test('without DB_SQLITE_JOURNAL_MODE the database keeps the rollback journal', async () => {
      const dir = path.resolve(TEST_TEMP_DIR, 'sqlite-default-journal')
      const { dbPath, results } = await runContenders(dir, 1, 10)
      expect(results[0]!.pragmas).toMatchObject({ journalMode: 'delete', busyTimeout: 5000 })
      const db = new SQLite(dbPath)
      try {
        expect(db.pragma('journal_mode', { simple: true })).toBe('delete')
      } finally {
        db.close()
      }
    }, 60_000)

    test.each([1, 2, 4])(
      'with WAL, %i concurrent writers never surface SQLITE_BUSY and keep leases exclusive',
      async (CONTENDERS) => {
        const dir = path.resolve(TEST_TEMP_DIR, `sqlite-multiprocess-${CONTENDERS}`)
        const { dbPath, results } = await runContenders(dir, CONTENDERS, ITERATIONS, 'wal')
        for (const result of results)
          expect(result.pragmas).toEqual({ journalMode: 'wal', synchronous: 1, busyTimeout: 5000 })

        const sum = (pick: (r: ContenderResult) => number) =>
          results.reduce((total, r) => total + pick(r), 0)
        const db = new SQLite(dbPath)
        try {
          const row = (id: string) =>
            db
              .prepare('select partCount, sizeBytes from storage_locations where id = ?')
              .get(id) as {
              partCount: number
              sizeBytes: number
            }
          // No lost update: lease holders and transactions never overlapped.
          expect(row('contended-merge-lease').partCount).toBe(sum((r) => r.leasesAcquired))
          expect(row('contended-transaction').sizeBytes).toBe(CONTENDERS * ITERATIONS)
          expect(sum((r) => r.readerLeases)).toBeGreaterThan(0)
          if (CONTENDERS > 1) expect(sum((r) => r.locationDeletes)).toBeGreaterThan(0)

          // Exactly one process applied each migration.
          const migrations = db.prepare('select name from kysely_migration').all() as {
            name: string
          }[]
          expect(new Set(migrations.map((m) => m.name)).size).toBe(migrations.length)
          expect(db.pragma('journal_mode', { simple: true })).toBe('wal')
          expect(db.prepare('select count(*) as n from merge_leases').get()).toEqual({ n: 0 })
          expect(db.prepare('select count(*) as n from storage_reader_leases').get()).toEqual({
            n: 0,
          })
        } finally {
          db.close()
        }
      },
      120_000,
    )
  },
)

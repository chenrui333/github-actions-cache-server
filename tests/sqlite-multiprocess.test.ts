import fs from 'node:fs/promises'
import path from 'node:path'
import SQLite from 'better-sqlite3'
import { execa } from 'execa'
import { describe, expect, test } from 'vitest'
import { TEST_TEMP_DIR } from './setup'

// Several processes share one SQLite file, as the workers of a `node-cluster`
// deployment do (NITRO_CLUSTER_WORKERS > 1). See ADR-0010.
const CONTENDERS = 4
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

describe.runIf((process.env.VITEST_DB_DRIVER ?? 'sqlite') === 'sqlite')(
  'sqlite shared by several processes',
  () => {
    test('concurrent writers never surface SQLITE_BUSY and keep leases exclusive', async () => {
      const dir = path.resolve(TEST_TEMP_DIR, 'sqlite-multiprocess')
      await fs.rm(dir, { recursive: true, force: true })
      await fs.mkdir(dir, { recursive: true })
      const dbPath = path.join(dir, 'shared.sqlite')

      // Fresh file: every contender also races the WAL switch and the migrations.
      const runs = await Promise.all(
        Array.from({ length: CONTENDERS }, () =>
          execa('tsx', ['tests/sqlite-contender.ts'], {
            preferLocal: true,
            env: {
              DB_DRIVER: 'sqlite',
              DB_SQLITE_PATH: dbPath,
              STORAGE_DRIVER: 'filesystem',
              STORAGE_FILESYSTEM_PATH: path.join(dir, 'storage'),
              API_BASE_URL: 'http://localhost:3000',
              CONTENDER_ITERATIONS: String(ITERATIONS),
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
        expect(result.pragmas).toEqual({ journalMode: 'wal', synchronous: 1, busyTimeout: 5000 })
        expect(result.transactions).toBe(ITERATIONS)
        expect(result.uploads).toBe(ITERATIONS)
      }

      const sum = (pick: (r: ContenderResult) => number) =>
        results.reduce((total, r) => total + pick(r), 0)
      const db = new SQLite(dbPath)
      try {
        const row = (id: string) =>
          db.prepare('select partCount, sizeBytes from storage_locations where id = ?').get(id) as {
            partCount: number
            sizeBytes: number
          }
        // No lost update: lease holders and transactions never overlapped.
        expect(row('contended-merge-lease').partCount).toBe(sum((r) => r.leasesAcquired))
        expect(row('contended-transaction').sizeBytes).toBe(CONTENDERS * ITERATIONS)
        expect(sum((r) => r.readerLeases)).toBeGreaterThan(0)
        expect(sum((r) => r.locationDeletes)).toBeGreaterThan(0)

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
    }, 120_000)
  },
)

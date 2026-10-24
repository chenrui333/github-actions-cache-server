import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

import { restoreCache, saveCache } from '@actions/cache'
import { SignJWT } from 'jose'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { TEST_TEMP_DIR } from './setup'

const testFilePath = path.join(TEST_TEMP_DIR, 'metrics.bin')

async function scrape() {
  const res = await fetch('http://localhost:3000/metrics')
  expect(res.status).toBe(200)
  expect(res.headers.get('content-type')).toContain('text/plain')
  return res.text()
}

function readCounter(text: string, series: string) {
  const line = text.split('\n').find((l) => l.startsWith(`${series} `))
  return line ? Number(line.slice(series.length + 1)) : 0
}

describe('prometheus metrics', () => {
  beforeAll(async () => {
    process.env.ACTIONS_CACHE_SERVICE_V2 = 'true'
    process.env.ACTIONS_RUNTIME_TOKEN = await new SignJWT({
      ac: JSON.stringify([{ Scope: 'refs/heads/main', Permission: 3 }]),
      repository_id: '123',
    })
      .setProtectedHeader({ alg: 'HS256' })
      .sign(crypto.createSecretKey('mock-secret-key', 'ascii'))
  })
  afterAll(() => {
    delete process.env.ACTIONS_CACHE_SERVICE_V2
    delete process.env.ACTIONS_RUNTIME_TOKEN
  })

  test('counts uploads, hits, misses and transferred bytes', async () => {
    const before = await scrape()
    // default process metrics are wired up
    expect(before).toContain('process_start_time_seconds')

    // upload
    await fs.writeFile(testFilePath, crypto.randomBytes(1024 * 1024))
    await saveCache([testFilePath], 'metrics-key')
    await fs.rm(testFilePath)

    // hit
    const hitKey = await restoreCache([testFilePath], 'metrics-key')
    expect(hitKey).toBe('metrics-key')
    await fs.rm(testFilePath)

    // miss
    const missKey = await restoreCache([testFilePath], `metrics-miss-${crypto.randomUUID()}`)
    expect(missKey).toBeUndefined()

    const after = await scrape()

    // Counters are shared with concurrent tests, so assert deltas, not equality.
    expect(readCounter(after, 'cache_uploads_total')).toBeGreaterThan(
      readCounter(before, 'cache_uploads_total'),
    )
    expect(readCounter(after, 'cache_requests_total{result="hit"}')).toBeGreaterThan(
      readCounter(before, 'cache_requests_total{result="hit"}'),
    )
    expect(readCounter(after, 'cache_requests_total{result="miss"}')).toBeGreaterThan(
      readCounter(before, 'cache_requests_total{result="miss"}'),
    )

    // Scrape-time gauge: present and non-negative (collect() ran without throwing).
    expect(after).toContain('cache_storage_bytes')
    expect(readCounter(after, 'cache_storage_bytes')).toBeGreaterThanOrEqual(0)

    // Every series the dashboards read is exposed, with or without observations.
    for (const name of [
      'cache_lookup_duration_seconds',
      'cache_downloads_total',
      'cache_merges_total',
      'cache_merges_skipped_total',
      'cache_merge_duration_seconds',
      'cache_merges_in_progress',
      'cache_merge_oldest_age_seconds',
      'cache_leases',
      's3_http_sockets',
      's3_http_queued_requests',
      's3_http_max_sockets',
      's3_socket_wait_seconds',
      's3_requests_total',
      's3_request_duration_seconds',
      's3_request_retries_total',
    ])
      expect(after, name).toMatch(new RegExp(`^# TYPE ${name} `, 'm'))

    for (const series of [
      'cache_lookup_duration_seconds_count{result="hit"}',
      'cache_lookup_duration_seconds_count{result="miss"}',
      'cache_downloads_total{result="completed"}',
    ])
      expect(readCounter(after, series), series).toBeGreaterThan(readCounter(before, series))
    expect(after).toContain('cache_downloads_total{result="idle_timeout"}')
    expect(after).toContain('cache_leases{type="merge",state="active"}')
    expect(after).toContain('cache_leases{type="reader",state="expired"}')

    // The first restore of an unmerged entry runs a streaming Merge in the background.
    const merged = 'cache_merges_total{kind="stream",result="success"}'
    await expect
      .poll(async () => readCounter(await scrape(), merged), { timeout: 10_000 })
      .toBeGreaterThan(readCounter(before, merged))

    if (process.env.VITEST_STORAGE_DRIVER === 's3') {
      const headOk = 's3_requests_total{command="HeadObject",outcome="ok"}'
      expect(readCounter(after, headOk)).toBeGreaterThan(readCounter(before, headOk))
      // Upload, restore and lookup each went through their own configured,
      // instrumented pool (the test endpoint is http://, so this also proves the
      // SDK's default agent is not used).
      for (const pool of ['writes', 'reads', 'control']) {
        const pooled =
          readCounter(after, `s3_http_sockets{pool="${pool}",state="active"}`) +
          readCounter(after, `s3_http_sockets{pool="${pool}",state="idle"}`)
        expect(pooled, `${pool} sockets`).toBeGreaterThan(0)
      }
      expect(readCounter(after, 's3_http_max_sockets{pool="reads"}')).toBe(50)
      expect(readCounter(after, 's3_http_max_sockets{pool="control"}')).toBe(16)
    }
  })
})

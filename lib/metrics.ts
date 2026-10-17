import type { Agent, ClientRequest } from 'node:http'
import { performance } from 'node:perf_hooks'
import { sql } from 'kysely'
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client'
import { getDatabase } from './db'

// Per-process registry. Correct for the shipped single-worker-per-container
// topology (Dockerfile: NITRO_CLUSTER_WORKERS=1) — scale with replicas and
// aggregate in PromQL. NITRO_CLUSTER_WORKERS>1 makes these counters per-worker
// because workers share the port round-robin. See ADR-0007.
export const metricsRegistry = new Registry()

collectDefaultMetrics({ register: metricsRegistry })

export const cacheRequestsTotal = new Counter({
  name: 'cache_requests_total',
  help: 'Cache download-URL lookups by result. A restore-key prefix match counts as a hit.',
  labelNames: ['result'],
  registers: [metricsRegistry],
})
// Materialise both series at 0 so dashboards see them before the first event.
cacheRequestsTotal.inc({ result: 'hit' }, 0)
cacheRequestsTotal.inc({ result: 'miss' }, 0)

export const cacheUploadsTotal = new Counter({
  name: 'cache_uploads_total',
  help: 'Cache uploads finalized into cache entries.',
  registers: [metricsRegistry],
})

// Computed at scrape time from the per-location payload sizes recorded at upload
// completion. Rows predating size tracking read as 0 until reconciled on the
// next startup. See ADR-0008.
export const cacheStorageBytes = new Gauge({
  name: 'cache_storage_bytes',
  help: 'Total bytes of finalized cache payloads tracked across storage locations.',
  registers: [metricsRegistry],
  async collect() {
    const db = await getDatabase()
    const { bytes } = await db
      .selectFrom('storage_locations')
      .select(sql<number>`coalesce(sum(${sql.ref('sizeBytes')}), 0)`.as('bytes'))
      .executeTakeFirstOrThrow()
    cacheStorageBytes.set(Number(bytes))
  },
})

const SECONDS_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]

export const cacheLookupDurationSeconds = new Histogram({
  name: 'cache_lookup_duration_seconds',
  help: 'Download-URL lookup handling time by result, including the storage existence check.',
  labelNames: ['result'],
  buckets: SECONDS_BUCKETS,
  registers: [metricsRegistry],
})

export const cacheDownloadsTotal = new Counter({
  name: 'cache_downloads_total',
  help: 'Proxied /download requests by outcome. client_gone: the client hung up before the body finished; idle_timeout: no bytes moved for DOWNLOAD_IDLE_TIMEOUT_MS.',
  labelNames: ['result'],
  registers: [metricsRegistry],
})
for (const result of ['completed', 'client_gone', 'idle_timeout', 'not_found', 'error'])
  cacheDownloadsTotal.inc({ result }, 0)

export type MergeKind = 'stream' | 'compose'

export const cacheMergesTotal = new Counter({
  name: 'cache_merges_total',
  help: 'Finished Merges by kind (stream: parts piped through this process; compose: server-side copy) and result.',
  labelNames: ['kind', 'result'],
  registers: [metricsRegistry],
})

export const cacheMergesSkippedTotal = new Counter({
  name: 'cache_merges_skipped_total',
  help: 'Merges not started because this process already runs MAX_CONCURRENT_MERGES; a later download retries them.',
  registers: [metricsRegistry],
})

export const cacheMergeDurationSeconds = new Histogram({
  name: 'cache_merge_duration_seconds',
  help: 'Wall time of finished Merges by kind and result.',
  labelNames: ['kind', 'result'],
  buckets: [1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600],
  registers: [metricsRegistry],
})

const mergeStartTimes = new Set<{ startedAt: number }>()

export const cacheMergesInProgress = new Gauge({
  name: 'cache_merges_in_progress',
  help: 'Merges this process is running now.',
  registers: [metricsRegistry],
  collect() {
    cacheMergesInProgress.set(mergeStartTimes.size)
  },
})

export const cacheMergeOldestAgeSeconds = new Gauge({
  name: 'cache_merge_oldest_age_seconds',
  help: 'Age of the longest-running in-progress Merge in this process; 0 when none is running.',
  registers: [metricsRegistry],
  collect() {
    let oldest = Infinity
    for (const { startedAt } of mergeStartTimes) oldest = Math.min(oldest, startedAt)
    cacheMergeOldestAgeSeconds.set(oldest === Infinity ? 0 : (performance.now() - oldest) / 1000)
  },
})

/** Call when a Merge starts; call the returned function exactly once when it settles. */
export function trackMerge(kind: MergeKind) {
  const entry = { startedAt: performance.now() }
  mergeStartTimes.add(entry)
  return (result: 'success' | 'failure') => {
    if (!mergeStartTimes.delete(entry)) return
    cacheMergesTotal.inc({ kind, result })
    cacheMergeDurationSeconds.observe(
      { kind, result },
      (performance.now() - entry.startedAt) / 1000,
    )
  }
}

// Lease rows, computed at scrape time. Expired rows mean a holder died or stopped
// renewing; a growing active count with no finished Merges means Merges are stuck.
export const cacheLeases = new Gauge({
  name: 'cache_leases',
  help: 'Merge and Storage Reader Lease rows by state (active: not yet expired).',
  labelNames: ['type', 'state'],
  registers: [metricsRegistry],
  async collect() {
    const db = await getDatabase()
    const now = Date.now()
    const count = (table: 'merge_leases' | 'storage_reader_leases') =>
      db
        .selectFrom(table)
        .select([
          sql<number>`count(*)`.as('total'),
          sql<number>`coalesce(sum(case when ${sql.ref('expiresAt')} < ${now} then 1 else 0 end), 0)`.as(
            'expired',
          ),
        ])
        .executeTakeFirstOrThrow()
    const [merge, reader] = await Promise.all([
      count('merge_leases'),
      count('storage_reader_leases'),
    ])
    for (const [type, row] of [
      ['merge', merge],
      ['reader', reader],
    ] as const) {
      const expired = Number(row.expired)
      cacheLeases.set({ type, state: 'expired' }, expired)
      cacheLeases.set({ type, state: 'active' }, Number(row.total) - expired)
    }
  },
})

// --- S3 client ---------------------------------------------------------------

/** reads: GetObject bodies; writes: uploads and Merges; control: HEAD/LIST/DELETE. */
export type S3Pool = 'reads' | 'writes' | 'control'

const s3Agents: { pool: S3Pool; agent: Agent }[] = []

export const s3HttpSockets = new Gauge({
  name: 's3_http_sockets',
  help: 'S3 agent sockets by pool and state (active: assigned to a request, idle: kept alive). Counts the agent view; a socket the remote end already closed still reads active until it is read or destroyed.',
  labelNames: ['pool', 'state'],
  registers: [metricsRegistry],
  collect() {
    const counts = new Map<S3Pool, { active: number; idle: number }>()
    for (const { pool, agent } of s3Agents) {
      const count = counts.get(pool) ?? { active: 0, idle: 0 }
      for (const sockets of Object.values(agent.sockets)) count.active += sockets?.length ?? 0
      for (const sockets of Object.values(agent.freeSockets)) count.idle += sockets?.length ?? 0
      counts.set(pool, count)
    }
    for (const [pool, { active, idle }] of counts) {
      s3HttpSockets.set({ pool, state: 'active' }, active)
      s3HttpSockets.set({ pool, state: 'idle' }, idle)
    }
  },
})

export const s3HttpQueuedRequests = new Gauge({
  name: 's3_http_queued_requests',
  help: 'Live S3 requests waiting for a free agent socket, by pool (maxSockets reached). Sustained > 0 means pool starvation.',
  labelNames: ['pool'],
  registers: [metricsRegistry],
  collect() {
    const counts = new Map<S3Pool, number>()
    for (const { pool, agent } of s3Agents) {
      let queued = counts.get(pool) ?? 0
      // Aborted requests stay in the queue until a socket frees; Node then skips them.
      for (const requests of Object.values(agent.requests))
        queued += requests?.filter((req) => !req.destroyed).length ?? 0
      counts.set(pool, queued)
    }
    for (const [pool, queued] of counts) s3HttpQueuedRequests.set({ pool }, queued)
  },
})

export const s3HttpMaxSockets = new Gauge({
  name: 's3_http_max_sockets',
  help: 'Configured S3 agent socket limit by pool (STORAGE_S3_MAX_SOCKETS, STORAGE_S3_CONTROL_MAX_SOCKETS).',
  labelNames: ['pool'],
  registers: [metricsRegistry],
})

export const s3SocketWaitSeconds = new Histogram({
  name: 's3_socket_wait_seconds',
  help: 'Time an S3 request waited for an agent socket, by pool. Requests aborted while queued are not observed; pair with s3_http_queued_requests.',
  labelNames: ['pool'],
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60],
  registers: [metricsRegistry],
})

/** Exposes `agent`'s pool on /metrics and times how long each request waits for a socket. */
export function instrumentS3Agent(pool: S3Pool, agent: Agent) {
  s3Agents.push({ pool, agent })
  s3HttpMaxSockets.set({ pool }, agent.maxSockets)
  // `addRequest` is where Node's Agent either hands out a socket or queues the
  // request; it is undocumented but the hook every agent wrapper relies on.
  const pooled = agent as Agent & { addRequest: (req: ClientRequest, options: unknown) => void }
  const addRequest = pooled.addRequest.bind(agent)
  pooled.addRequest = (req, options) => {
    const queuedAt = performance.now()
    req.once('socket', () =>
      s3SocketWaitSeconds.observe({ pool }, (performance.now() - queuedAt) / 1000),
    )
    addRequest(req, options)
  }
}

export const s3RequestsTotal = new Counter({
  name: 's3_requests_total',
  help: 'S3 commands by outcome (ok, not_found, aborted, timeout, network, error).',
  labelNames: ['command', 'outcome'],
  registers: [metricsRegistry],
})

export const s3RequestDurationSeconds = new Histogram({
  name: 's3_request_duration_seconds',
  help: 'S3 command time until the response headers, including socket wait and SDK retries; excludes streaming a GetObject body.',
  labelNames: ['command'],
  buckets: SECONDS_BUCKETS,
  registers: [metricsRegistry],
})

export const s3RequestRetriesTotal = new Counter({
  name: 's3_request_retries_total',
  help: 'SDK retry attempts beyond the first, by command.',
  labelNames: ['command'],
  registers: [metricsRegistry],
})

const S3_NETWORK_ERROR_CODES: Record<string, true> = {
  ECONNRESET: true,
  ECONNREFUSED: true,
  EPIPE: true,
  ENOTFOUND: true,
  EAI_AGAIN: true,
}

function s3Outcome(err: unknown) {
  const { name, code, $metadata } = (err ?? {}) as {
    name?: string
    code?: string
    $metadata?: { httpStatusCode?: number }
  }
  if (name === 'AbortError') return 'aborted'
  // The SDK renames ECONNRESET/EPIPE to TimeoutError, so check the code first.
  if (S3_NETWORK_ERROR_CODES[code ?? '']) return 'network'
  if (name === 'TimeoutError' || name === 'RequestTimeout' || code === 'ETIMEDOUT') return 'timeout'
  if (name === 'NotFound' || name === 'NoSuchKey' || $metadata?.httpStatusCode === 404)
    return 'not_found'
  return 'error'
}

/** Records one S3 command: `err` is the thrown error, absent on success. */
export function observeS3Request(
  commandName: string,
  startedAt: number,
  attempts: number | undefined,
  err?: unknown,
) {
  const command = commandName.replace(/Command$/, '')
  s3RequestDurationSeconds.observe({ command }, (performance.now() - startedAt) / 1000)
  s3RequestsTotal.inc({ command, outcome: err === undefined ? 'ok' : s3Outcome(err) })
  if (attempts && attempts > 1) s3RequestRetriesTotal.inc({ command }, attempts - 1)
}

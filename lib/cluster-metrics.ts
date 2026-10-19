import type { Registry } from 'prom-client'
import cluster from 'node:cluster'
import { randomUUID } from 'node:crypto'
import { AggregatorRegistry } from 'prom-client'

// Workers of a `node-cluster` deployment share the port round-robin, so a scrape
// lands on one of them. The worker answering /metrics asks the primary, which
// collects every worker's registry over cluster IPC with prom-client's
// AggregatorRegistry and returns the merged text. See ADR-0011.

const AGGREGATE_REQUEST = 'cache-server:aggregate-metrics'
const AGGREGATE_RESPONSE = 'cache-server:aggregated-metrics'
// prom-client gives up on unresponsive workers after 5s; leave room for that error.
const AGGREGATE_TIMEOUT_MS = 10_000

interface AggregateResponse {
  type: typeof AGGREGATE_RESPONSE
  id: string
  metrics?: string
  error?: string
}

/** Primary: answer workers' aggregation requests. */
export function serveClusterMetrics() {
  const aggregator = new AggregatorRegistry()
  cluster.on('message', (worker, message: { type?: unknown; id?: unknown } | null) => {
    if (message?.type !== AGGREGATE_REQUEST || typeof message.id !== 'string') return
    const id = message.id
    const reply = (response: Omit<AggregateResponse, 'type' | 'id'>) => {
      if (worker.isConnected()) worker.send({ type: AGGREGATE_RESPONSE, id, ...response })
    }
    aggregator.clusterMetrics().then(
      (metrics) => reply({ metrics }),
      (err: unknown) => reply({ error: err instanceof Error ? err.message : String(err) }),
    )
  })
}

const pending = new Map<
  string,
  { resolve: (metrics: string) => void; reject: (err: Error) => void; timer: NodeJS.Timeout }
>()

/** Worker: let the primary collect `registry` and route aggregated replies back. */
export function joinClusterMetrics(registry: Registry) {
  AggregatorRegistry.setRegistries(registry)
  // The constructor installs the worker's listener for the primary's collection requests.
  // eslint-disable-next-line no-new
  new AggregatorRegistry()
  process.on('message', (message: Partial<AggregateResponse> | null) => {
    if (message?.type !== AGGREGATE_RESPONSE || typeof message.id !== 'string') return
    const request = pending.get(message.id)
    if (!request) return
    pending.delete(message.id)
    clearTimeout(request.timer)
    if (typeof message.metrics === 'string') request.resolve(message.metrics)
    else request.reject(new Error(`Metrics aggregation failed: ${message.error ?? 'no metrics'}`))
  })
}

/** Worker: metrics of every worker, aggregated by the primary. */
export function aggregatedClusterMetrics() {
  const id = randomUUID()
  return new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id)
      reject(new Error('Timed out waiting for aggregated metrics from the cluster primary'))
    }, AGGREGATE_TIMEOUT_MS)
    pending.set(id, { resolve, reject, timer })
    process.send!({ type: AGGREGATE_REQUEST, id })
  })
}

import cluster from 'node:cluster'
import { aggregatedClusterMetrics } from '~/lib/cluster-metrics'
import { metricsRegistry } from '~/lib/metrics'

export default defineEventHandler(async (event) => {
  // AggregatorRegistry emits the same text format as the per-process registry.
  setHeader(event, 'Content-Type', metricsRegistry.contentType)
  return cluster.isWorker ? await aggregatedClusterMetrics() : await metricsRegistry.metrics()
})

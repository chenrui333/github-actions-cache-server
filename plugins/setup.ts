/* eslint-disable unicorn/no-process-exit */
import cluster from 'node:cluster'

import { clusterMetricsAggregated, serveClusterMetrics } from '~/lib/cluster-metrics'
import { getDatabase } from '~/lib/db'
import { logger } from '~/lib/logger'
import { getStorage } from '~/lib/storage'

export default defineNitroPlugin(async (nitro) => {
  const version = useRuntimeConfig().version
  // The `node-cluster` primary only forks and supervises workers; the
  // `node-server` preset (tests) runs everything in one primary process.
  const isClusterPrimary = cluster.isPrimary && import.meta.preset === 'node-cluster'
  if (cluster.isPrimary) {
    logger.info(`🚀 Starting GitHub Actions Cache Server (${version})`)

    for (const signal of ['SIGTERM', 'SIGINT'] satisfies NodeJS.Signals[]) {
      process.on(signal, () => {
        const workers = Object.values(cluster.workers ?? {})
        for (const worker of workers) {
          worker?.process.kill(signal)
        }
      })
    }
  }
  if (isClusterPrimary && clusterMetricsAggregated) serveClusterMetrics()

  const db = await getDatabase().catch((err) => {
    logger.error('Failed to initialize database:', err)
    process.exit(1)
  })
  const storage = await getStorage().catch((err) => {
    logger.error('Failed to initialize storage:', err)
    process.exit(1)
  })

  // The primary initializes the database and storage only to fail the container
  // fast on bad configuration; it serves no requests and runs no tasks, so it
  // does not keep a connection to the database open.
  if (isClusterPrimary) await db.destroy()

  nitro.hooks.hook('close', async () => {
    await storage.waitForOngoingMerges()
    // Closing the last SQLite connection checkpoints the WAL into the database file.
    await db.destroy()
  })

  if (process.send && cluster.isPrimary) process.send('nitro:ready')
})

import type { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { z } from 'zod'
import { env } from '~/lib/env'
import { logger } from '~/lib/logger'
import { cacheDownloadsTotal } from '~/lib/metrics'
import { getStorage } from '~/lib/storage'

const pathParamsSchema = z.object({
  cacheEntryId: z.string(),
})

const CLIENT_GONE_ERROR_CODES: Record<string, true> = {
  ABORT_ERR: true,
  ERR_STREAM_PREMATURE_CLOSE: true,
  ERR_STREAM_UNABLE_TO_PIPE: true,
}

export default defineEventHandler(async (event) => {
  const parsedPathParams = pathParamsSchema.safeParse(event.context.params)
  if (!parsedPathParams.success)
    throw createError({
      statusCode: 400,
      statusMessage: `Invalid path parameters: ${parsedPathParams.error.message}`,
    })

  const { cacheEntryId } = parsedPathParams.data
  const { res } = event.node

  // The client can hang up while storage is still answering (a GetObject queued
  // behind a full socket pool). Abort the backend read then, so it gives its
  // connection back instead of returning a body nobody will read.
  const abort = new AbortController()
  const onClose = () => {
    if (!res.writableFinished) abort.abort()
  }
  res.once('close', onClose)

  let stream: Readable | undefined
  let idleTimer: NodeJS.Timeout | undefined
  try {
    const storage = await getStorage()
    stream = await storage.download(cacheEntryId, { signal: abort.signal })
    if (!stream)
      throw createError({
        statusCode: 404,
        message: 'Cache file not found',
      })

    // Not h3's `sendStream`: its web-stream path neither applies backpressure nor
    // notices the client hanging up, so an aborted download keeps draining the
    // backend read into a socket nobody is reading.
    event._handled = true
    // `pipeline` refuses an already-closed destination without taking ownership
    // of the source (Node 25: ERR_STREAM_UNABLE_TO_PIPE), so check first.
    if (res.destroyed || res.closed) {
      stream.destroy()
      cacheDownloadsTotal.inc({ result: 'client_gone' })
      logger.debug(`Client aborted /download/${cacheEntryId} before storage answered`)
      return
    }
    // The HTTP server has no idle timeout, so a client that stops reading but
    // keeps the connection open would hold the backend read, its Storage Reader
    // Lease and, on a first restore, the Merge forever. Bytes moving at any rate
    // keep the download alive.
    if (env.DOWNLOAD_IDLE_TIMEOUT_MS > 0) {
      const body = stream
      idleTimer = setTimeout(() => {
        body.destroy(
          Object.assign(new Error(`No bytes sent for ${env.DOWNLOAD_IDLE_TIMEOUT_MS} ms`), {
            code: 'ERR_DOWNLOAD_IDLE_TIMEOUT',
          }),
        )
      }, env.DOWNLOAD_IDLE_TIMEOUT_MS)
      idleTimer.unref()
      body.on('data', () => idleTimer?.refresh())
    }
    await pipeline(stream, res)
    cacheDownloadsTotal.inc({ result: 'completed' })
  } catch (err) {
    // Every exit that did not finish the pipe must release the backend read: an
    // unread body pins its storage connection and the reader lease it carries.
    stream?.destroy()
    const { code, name } = err as NodeJS.ErrnoException
    if (code === 'ERR_DOWNLOAD_IDLE_TIMEOUT') {
      cacheDownloadsTotal.inc({ result: 'idle_timeout' })
      logger.warn(`Download /download/${cacheEntryId} closed: ${(err as Error).message}`)
      return
    }
    if (abort.signal.aborted || name === 'AbortError' || CLIENT_GONE_ERROR_CODES[code ?? '']) {
      cacheDownloadsTotal.inc({ result: 'client_gone' })
      // Expected on long downloads (cancelled jobs, parallel runners) and there
      // is nowhere left to report it.
      logger.debug(`Client aborted /download/${cacheEntryId}: ${(err as Error).message}`)
      return
    }
    cacheDownloadsTotal.inc({
      result: (err as { statusCode?: number }).statusCode === 404 ? 'not_found' : 'error',
    })
    // Headers are already out, so this cannot become an HTTP error response;
    // Nitro's handler would call `setResponseHeaders` after the fact and crash
    // with ERR_HTTP_HEADERS_SENT.
    if (res.headersSent) {
      logger.error(`Download stream failed for ${cacheEntryId}`, { error: err })
      return
    }
    throw err
  } finally {
    clearTimeout(idleTimer)
    res.off('close', onClose)
  }
})

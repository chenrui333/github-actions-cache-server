import type { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { z } from 'zod'
import { logger } from '~/lib/logger'
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
      logger.debug(`Client aborted /download/${cacheEntryId} before storage answered`)
      return
    }
    await pipeline(stream, res)
  } catch (err) {
    // Every exit that did not finish the pipe must release the backend read: an
    // unread body pins its storage connection and the reader lease it carries.
    stream?.destroy()
    const { code, name } = err as NodeJS.ErrnoException
    if (abort.signal.aborted || name === 'AbortError' || CLIENT_GONE_ERROR_CODES[code ?? '']) {
      // Expected on long downloads (cancelled jobs, parallel runners) and there
      // is nowhere left to report it.
      logger.debug(`Client aborted /download/${cacheEntryId}: ${(err as Error).message}`)
      return
    }
    // Headers are already out, so this cannot become an HTTP error response;
    // Nitro's handler would call `setResponseHeaders` after the fact and crash
    // with ERR_HTTP_HEADERS_SENT.
    if (res.headersSent) {
      logger.error(`Download stream failed for ${cacheEntryId}`, { error: err })
      return
    }
    throw err
  } finally {
    res.off('close', onClose)
  }
})

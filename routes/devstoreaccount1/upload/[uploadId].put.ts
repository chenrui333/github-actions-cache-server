import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'

import { z } from 'zod'

import { logger } from '~/lib/logger'
import { getStorage } from '~/lib/storage'

const pathParamsSchema = z.object({
  uploadId: z.coerce.number(),
})

export default defineEventHandler(async (event) => {
  const parsedPathParams = pathParamsSchema.safeParse(event.context.params)
  if (!parsedPathParams.success)
    throw createError({
      statusCode: 400,
      statusMessage: `Invalid path parameters: ${parsedPathParams.error.message}`,
    })

  const { uploadId } = parsedPathParams.data
  const query = getQuery(event)
  // The capability issued in `signed_upload_url`. Clients keep the URL's query
  // and append their own parameters, as they do with an Azure SAS.
  const capability = typeof query.sig === 'string' ? query.sig : undefined

  // A blocklist commits the blocks already stored; there is no part to write.
  let chunkIndex: number | undefined
  if (query.comp !== 'blocklist') {
    const blockId = query.blockid as string
    // if no block id, upload smaller than chunk size
    chunkIndex = blockId ? getChunkIndexFromBlockId(blockId) : 0
    if (chunkIndex === undefined)
      throw createError({
        statusCode: 400,
        statusMessage: `Invalid block id: ${blockId}`,
      })
  }

  let authorized: boolean
  try {
    const storage = await getStorage()
    authorized = await storage.authorizeUpload(uploadId, capability)
    // Not h3's `getRequestWebStream`: it enqueues every `data` event and never pauses
    // the request, so a body arriving faster than storage accepts it is buffered in
    // memory in full. Reading `req` directly lets storage pace the client over TCP.
    if (authorized && chunkIndex !== undefined)
      await storage.uploadPart(uploadId, chunkIndex, event.node.req)
  } catch (err) {
    // Logged here and rethrown as handled: Nitro logs unhandled errors with the
    // request URL, which carries the capability.
    logger.error(`Failed to store upload ${uploadId}`, err)
    throw createError({ statusCode: 500, statusMessage: 'Failed to store upload' })
  }
  if (!authorized) throw createError({ statusCode: 403, statusMessage: 'Forbidden' })

  // prevent random EOF error with in tonistiigi/go-actions-cache caused by missing request id
  setHeader(event, 'x-ms-request-id', randomUUID())
  setResponseStatus(event, 201)
})

function getChunkIndexFromBlockId(blockIdBase64: string) {
  const base64Decoded = Buffer.from(blockIdBase64, 'base64')

  // 64 bytes used by docker buildx
  // 48 bytes used by everything else
  if (base64Decoded.length === 64) return base64Decoded.readUInt32BE(16)

  if (base64Decoded.length === 48) {
    const decoded = base64Decoded.toString('utf8')

    // slice off uuid and convert to number
    const index = Number.parseInt(decoded.slice(36))
    if (Number.isNaN(index)) return

    return index
  }
}

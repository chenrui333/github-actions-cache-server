import { randomUUID } from 'node:crypto'

import { z } from 'zod'

import { logger } from '~/lib/logger'
import { getStorage } from '~/lib/storage'
import { parseBlockList, requestBody, UploadProtocolError } from '~/lib/upload-protocol'

const pathParamsSchema = z.object({
  uploadId: z.coerce.number().int().positive().max(Number.MAX_SAFE_INTEGER),
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

  try {
    const storage = await getStorage()
    if (!(await storage.authorizeUpload(uploadId, capability)))
      throw new UploadProtocolError(403, 'Forbidden')
    if (query.comp === 'blocklist') {
      const blocks = await parseBlockList(requestBody(event.node.req))
      await storage.commitBlockList(uploadId, capability, blocks)
    } else if (query.comp === 'block' && typeof query.blockid === 'string') {
      await storage.stageBlock(uploadId, capability, query.blockid, requestBody(event.node.req))
    } else if (query.comp === undefined && query.blockid === undefined) {
      await storage.putBlob(uploadId, capability, requestBody(event.node.req))
    } else {
      throw new UploadProtocolError(400, 'Unsupported Azure upload operation')
    }
  } catch (err) {
    // Never let Nitro log an upload URL carrying its capability.
    if (err instanceof UploadProtocolError)
      throw createError({ statusCode: err.statusCode, statusMessage: err.message })
    logger.error(`Failed to store upload ${uploadId}`, err)
    throw createError({ statusCode: 500, statusMessage: 'Failed to store upload' })
  }

  // prevent random EOF error with in tonistiigi/go-actions-cache caused by missing request id
  setHeader(event, 'x-ms-request-id', randomUUID())
  setResponseStatus(event, 201)
})

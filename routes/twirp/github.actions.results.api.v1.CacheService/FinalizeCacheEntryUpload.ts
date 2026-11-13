import { z } from 'zod'
import { CACHE_PERMISSION_WRITE, getCacheScope } from '~/lib/scope'
import { getStorage } from '~/lib/storage'
import { readTwirpRequest, sendTwirpResponse, TwirpMessage } from '~/lib/twirp'

const bodySchema = z.object({
  key: z.string().min(1),
  version: z.string().min(1),
  size_bytes: z.coerce.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER).optional(),
})

export default defineEventHandler(async (event) => {
  const { scopes, repoId } = await getCacheScope(event)

  const { key, version, size_bytes } = await readTwirpRequest(
    event,
    bodySchema,
    TwirpMessage.FinalizeCacheEntryUploadRequest,
  )

  const storage = await getStorage()
  const writeScope = scopes.find((s) => (s.Permission & CACHE_PERMISSION_WRITE) !== 0)
  if (!writeScope)
    throw createError({ statusCode: 403, message: 'No scope with write permission found' })

  const upload = await storage.acknowledgeUpload({ key, version, scope: writeScope.Scope, repoId })
  if (!upload)
    throw createError({
      statusCode: 404,
      statusMessage: 'Completed upload not found',
    })

  if (size_bytes !== undefined && size_bytes !== upload.sizeBytes)
    throw createError({ statusCode: 409, statusMessage: 'Completed upload size does not match' })

  return sendTwirpResponse(
    event,
    { ok: true, entry_id: upload.uploadId.toString() },
    TwirpMessage.FinalizeCacheEntryUploadResponse,
  )
})

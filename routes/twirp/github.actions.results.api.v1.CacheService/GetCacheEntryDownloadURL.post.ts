import { filter, map, pipe, prop, sortBy } from 'remeda'
import { z } from 'zod'
import { cacheRequestsTotal } from '~/lib/metrics'
import { CACHE_PERMISSION_READ, getCacheScope } from '~/lib/scope'
import { getStorage } from '~/lib/storage'
import { readTwirpRequest, sendTwirpResponse, TwirpMessage } from '~/lib/twirp'

const bodySchema = z.object({
  key: z.string().min(1),
  restore_keys: z.array(z.string()).nullish().optional(),
  version: z.string().min(1),
})

export default defineEventHandler(async (event) => {
  const { scopes, repoId } = await getCacheScope(event)
  // Scopes are bit flags; a write-only scope must not be read from.
  const readScopes = pipe(
    scopes,
    filter((s) => (s.Permission & CACHE_PERMISSION_READ) !== 0),
    sortBy([prop('Permission'), 'desc']),
    map(prop('Scope')),
  )
  if (readScopes.length === 0)
    throw createError({ statusCode: 403, message: 'No scope with read permission found' })

  const { key, restore_keys, version } = await readTwirpRequest(
    event,
    bodySchema,
    TwirpMessage.GetCacheEntryDownloadURLRequest,
  )

  const storage = await getStorage()
  const match = await storage.getCacheEntryWithDownloadUrl({
    keys: [key, ...(restore_keys ?? [])],
    version,
    scopes: readScopes,
    repoId,
  })

  cacheRequestsTotal.inc({ result: match ? 'hit' : 'miss' })

  return sendTwirpResponse(
    event,
    match
      ? { ok: true, signed_download_url: match.downloadUrl, matched_key: match.cacheEntry.key }
      : { ok: false },
    TwirpMessage.GetCacheEntryDownloadURLResponse,
  )
})

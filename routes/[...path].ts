import { env } from '~/lib/env'
import { loggedPath } from '~/lib/helpers'
import { logger } from '~/lib/logger'

// This server's own Upload URLs. Only `PUT` is routed; any other method lands
// here, and forwarding it would hand the URL's capability to the upstream.
const UPLOAD_PATH = /^\/(?:devstoreaccount1\/)?upload\//

export default defineEventHandler(async (event) => {
  const path = loggedPath(event)
  if (UPLOAD_PATH.test(path))
    throw createError({ statusCode: 405, statusMessage: 'Method Not Allowed' })

  logger.debug('proxying unknown path', path, 'to', env.DEFAULT_ACTIONS_RESULTS_URL)
  return proxyRequest(event, `${env.DEFAULT_ACTIONS_RESULTS_URL}${event.path}`)
})

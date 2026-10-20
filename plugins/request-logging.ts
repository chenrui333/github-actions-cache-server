import type { H3Event } from 'h3'
import { H3Error } from 'h3'
import { env } from '~/lib/env'
import { logger } from '~/lib/logger'

/**
 * The request path without its query string. Upload URLs carry their
 * capability in the query (`?sig=`), so it must never reach the logs.
 */
function loggedPath(event: H3Event) {
  return event.path.split('?', 1)[0]
}

export default defineNitroPlugin((nitro) => {
  nitro.hooks.hook('error', (error, { event }) => {
    if (!event) {
      logger.error(error)
      return
    }

    logger.error(
      `Response: ${event.method} ${loggedPath(event)} > ${error instanceof H3Error ? error.statusCode : '[no status code]'}\n`,
      error,
    )
  })

  if (env.DEBUG) {
    nitro.hooks.hook('request', (event) => {
      logger.debug(`Request: ${event.method} ${loggedPath(event)}`)
    })
    nitro.hooks.hook('afterResponse', (event) => {
      logger.debug(`Response: ${event.method} ${loggedPath(event)} > ${getResponseStatus(event)}`)
    })
  }
})

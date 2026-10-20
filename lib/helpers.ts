import type { H3Event } from 'h3'
import { customAlphabet } from 'nanoid'

const generate = customAlphabet('0123456789', 10)

export function generateNumberId() {
  return Number.parseInt(generate())
}

/**
 * The request path without its query string, for logging. Upload URLs carry
 * their capability in the query (`?sig=`), so it must never reach the logs.
 */
export function loggedPath(event: H3Event) {
  return event.path.split('?', 1)[0]
}

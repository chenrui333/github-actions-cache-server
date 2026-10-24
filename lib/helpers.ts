import type { H3Event } from 'h3'
import { Buffer } from 'node:buffer'
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto'
import { customAlphabet } from 'nanoid'

const generate = customAlphabet('0123456789', 10)

export function generateNumberId() {
  return Number.parseInt(generate())
}

/**
 * The request path without its query string, for logging. A query can carry
 * credentials (signed URLs, SAS-style parameters), so it must never reach the logs.
 */
export function loggedPath(event: H3Event) {
  return event.path.split('?', 1)[0]
}

/** The key, version, scope and repo that at most one Upload may hold at a time. */
export interface Reservation {
  key: string
  version: string
  scope: string
  repoId: string
}

export function reservationHash({ key, version, scope, repoId }: Reservation) {
  return createHash('sha256')
    .update(JSON.stringify([key, version, scope, repoId]))
    .digest('hex')
}

/**
 * A new Upload capability: 256 random bits, sent in the Upload URL's query
 * (`?sig=`). Upload URLs are fetched by Azure Blob-style clients that send no
 * Actions token, so the capability is their only authorization.
 */
export function generateUploadCapability() {
  return randomBytes(32).toString('hex')
}

/** What is stored of a capability, so neither the database nor its query log can reproduce a working Upload URL. */
export function hashUploadCapability(capability: string) {
  return createHash('sha256').update(capability).digest('hex')
}

/** Whether `capability` is the one whose hash was stored, compared in constant time. */
export function uploadCapabilityMatches(
  capabilityHash: string | null | undefined,
  capability: string | undefined,
) {
  if (!capabilityHash || !capability) return false
  const expected = Buffer.from(capabilityHash, 'hex')
  const actual = createHash('sha256').update(capability).digest()
  return expected.length === actual.length && timingSafeEqual(expected, actual)
}

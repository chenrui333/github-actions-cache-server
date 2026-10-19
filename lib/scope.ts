import type { H3Event } from 'h3'
import * as jose from 'jose'
import { hasAtLeast } from 'remeda'
import { z } from 'zod'
import { env } from './env'
import { logger } from './logger'

const issuer = env.ACTIONS_TOKEN_ISSUER.replace(/\/$/, '')

const fallbackJwksUrl = `${issuer}/.well-known/jwks`

/**
 * The JWKS URL can't be derived from the issuer: an enterprise with a custom
 * issuer value (`{host}/{enterpriseSlug}`) still serves its JWKS at `{host}`.
 * Ask the OIDC discovery document instead.
 */
export async function discoverJwksUrl() {
  const discoveryUrl = `${issuer}/.well-known/openid-configuration`

  const res = await fetch(discoveryUrl)
  if (!res.ok) throw new Error(`Unexpected status ${res.status} ${res.statusText}`)

  const config = (await res.json()) as { jwks_uri?: unknown }
  if (typeof config.jwks_uri !== 'string')
    throw new Error(`Discovery document at ${discoveryUrl} has no \`jwks_uri\``)

  return config.jwks_uri
}

const createJwks = (url: string) => jose.createRemoteJWKSet(new URL(url))

const overrideJwks = env.ACTIONS_TOKEN_JWKS_URL ? createJwks(env.ACTIONS_TOKEN_JWKS_URL) : undefined
const fallbackJwks = createJwks(fallbackJwksUrl)
// holder object instead of a bare `let`, which can't be assigned to from inside
// a function (`unicorn/no-top-level-assignment-in-function`)
const cache: { jwks?: jose.JWTVerifyGetKey } = {}

async function getJwks() {
  if (overrideJwks) return overrideJwks
  if (cache.jwks) return cache.jwks

  try {
    return (cache.jwks = createJwks(await discoverJwksUrl()))
  } catch (err) {
    logger.warn(
      `OIDC discovery failed, falling back to ${fallbackJwksUrl}. Set ACTIONS_TOKEN_JWKS_URL if token validation keeps failing.`,
      err,
    )
    // Deliberately not cached, so the next request retries discovery instead of
    // pinning the process to a URL that may well be the wrong one.
    return fallbackJwks
  }
}

function getBearerToken(event: H3Event) {
  const authHeader = getHeader(event, 'authorization')
  if (!authHeader || !authHeader.startsWith('Bearer ')) return

  return authHeader.slice(7)
}

// Unset means no audience check. GitHub-issued runtime tokens carry a
// deployment-specific `aud`, so pinning it restricts the server to one
// installation's tokens; a comma-separated list allows several.
const audience = env.ACTIONS_TOKEN_AUDIENCE?.split(',')
  .map((value) => value.trim())
  .filter(Boolean)

async function verifyGitHubActionsToken(token: string) {
  if (env.SKIP_TOKEN_VALIDATION) {
    logger.warn('Token validation is disabled. This should not be used in production!')
    return jose.decodeJwt(token)
  }

  const { payload } = await jose.jwtVerify(token, await getJwks(), {
    issuer,
    audience: audience?.length ? audience : undefined,
    // jose checks `exp` and `nbf` only when present; a token without `exp`
    // would otherwise never expire.
    requiredClaims: ['exp'],
  })
  return payload
}

/**
 * The reason a token was rejected, safe to log and return: never the token or
 * its claims. jose errors carry the decoded payload, so they must not become
 * the `cause` of a logged error.
 */
function describeTokenError(err: unknown) {
  if (err instanceof jose.errors.JWTClaimValidationFailed || err instanceof jose.errors.JWTExpired)
    return `${err.code} (${err.claim})`
  if (err instanceof jose.errors.JOSEError) return err.code
  return err instanceof Error ? err.name : 'unknown error'
}

export const CACHE_PERMISSION_READ = 1
export const CACHE_PERMISSION_WRITE = 2

const cacheScopesSchema = z.array(
  z.object({
    Scope: z.string().min(1),
    Permission: z.number().int().nonnegative(),
  }),
)

function parseJsonScopes(json: string) {
  let value: unknown
  try {
    value = JSON.parse(json)
  } catch {
    // no `cause`: the parser's message quotes the claim
    throw createError({ statusCode: 401, message: 'Invalid JSON in cache scopes' })
  }

  const parsed = cacheScopesSchema.safeParse(value)
  if (!parsed.success) throw createError({ statusCode: 401, message: 'Invalid cache scopes' })
  return parsed.data
}

export async function getCacheScope(event: H3Event) {
  const token = getBearerToken(event)
  if (!token)
    throw createError({ statusCode: 401, message: 'Authorization header missing or malformed' })

  const decoded = await verifyGitHubActionsToken(token).catch((err) => {
    throw createError({ statusCode: 401, message: `Invalid token: ${describeTokenError(err)}` })
  })

  const scopesJson = decoded.ac
  if (!scopesJson || typeof scopesJson !== 'string')
    throw createError({ statusCode: 401, message: 'Token does not contain cache scopes' })

  const scopes = parseJsonScopes(scopesJson)
  if (!hasAtLeast(scopes, 1))
    throw createError({ statusCode: 401, message: 'Token does not contain any cache scopes' })

  const repoId = decoded.repository_id
  if (!repoId || typeof repoId !== 'string')
    throw createError({ statusCode: 401, message: 'Token does not contain repository id' })

  return {
    scopes,
    repoId,
  }
}

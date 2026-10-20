import type { ResultPromise } from 'execa'
import type { KeyObject } from 'node:crypto'
import type { Server } from 'node:http'
import type { AddressInfo } from 'node:net'

import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { setTimeout as sleep } from 'node:timers/promises'

import { type } from 'arkenv'
import { execa } from 'execa'
import { exportJWK, SignJWT } from 'jose'
import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import {
  assertDirectDownloadsRequireTokenValidation,
  envBaseSchema,
  parseActionsTokenAudience,
} from '~/lib/schemas'

// A second server with token validation on, verifying against a local issuer
// whose keys are generated here, so these tests never reach GitHub.
const PORT = 3011
const BASE_URL = `http://localhost:${PORT}`
const TWIRP = `${BASE_URL}/twirp/github.actions.results.api.v1.CacheService`
const URL_TTL_SECONDS = 120
const KID = 'test-key'
const AUDIENCE = 'test-audience'

const trusted = generateKeyPairSync('rsa', { modulusLength: 2048 })
const untrusted = generateKeyPairSync('rsa', { modulusLength: 2048 })

const repoA = `repo-a-${randomUUID()}`
const repoB = `repo-b-${randomUUID()}`
const scopeMarker = `refs/heads/marker-${randomUUID()}`
// Only ever inside a token, so finding it in the logs means decoded claims were logged.
const claimMarker = `claim-marker-${randomUUID()}`

// holder object instead of bare `let`s (`unicorn/no-top-level-assignment-in-function`)
const issuer: {
  url: string
  server?: Server
  keys: object[]
  jwksFetchedAt: number[]
} = { url: '', keys: [], jwksFetchedAt: [] }

async function publicJwk(key: KeyObject, kid: string) {
  return { ...(await exportJWK(key)), kid, alg: 'RS256', use: 'sig' }
}

async function startIssuer() {
  issuer.keys.push(await publicJwk(trusted.publicKey, KID))
  const server = createServer((req, res) => {
    res.setHeader('content-type', 'application/json')
    if (req.url === '/.well-known/openid-configuration')
      return res.end(
        JSON.stringify({ issuer: issuer.url, jwks_uri: `${issuer.url}/.well-known/jwks` }),
      )
    if (req.url === '/.well-known/jwks') {
      issuer.jwksFetchedAt.push(Date.now())
      return res.end(JSON.stringify({ keys: issuer.keys }))
    }
    // a JWKS endpoint that is down
    res.statusCode = req.url === '/broken-jwks' ? 503 : 404
    res.end('{}')
  })
  const listening = Promise.withResolvers<void>()
  server.listen(0, '127.0.0.1', listening.resolve)
  await listening.promise
  issuer.server = server
  issuer.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

function encodeSegment(value: object) {
  return Buffer.from(JSON.stringify(value)).toString('base64url')
}

// Same database and storage as the main test server, so whatever this server
// stores stays authorized for the other suites' orphaned-storage checks.
function serverEnv(overrides: Record<string, string>) {
  return {
    ...process.env,
    NITRO_PORT: String(PORT),
    API_BASE_URL: BASE_URL,
    DISABLE_CLEANUP_JOBS: 'true',
    ...overrides,
  }
}

type ServerProcess = ResultPromise<{ node: true; ipc: true; all: true; reject: false }>

function startServer(overrides: Record<string, string>): ServerProcess {
  return execa({
    node: true,
    ipc: true,
    all: true,
    reject: false,
    env: serverEnv(overrides),
  })`.output/server/index.mjs`
}

/** Resolves `ready` once the server signals readiness, or `exited` if it dies first. */
async function readyOrExited(server: ServerProcess) {
  return Promise.race([
    server
      .getOneMessage({ filter: (message) => message === 'nitro:ready' })
      .then(() => 'ready' as const)
      .catch(() => 'exited' as const),
    server.then(() => 'exited' as const),
  ])
}

type Claims = Record<string, unknown>

async function mint(
  claims: Claims,
  {
    key = trusted.privateKey,
    kid = KID,
    iss = issuer.url,
    aud = AUDIENCE,
    exp = '10m',
    nbf,
  }: {
    key?: KeyObject
    kid?: string
    iss?: string
    aud?: string | null
    exp?: string | number | null
    nbf?: string | number
  } = {},
) {
  const jwt = new SignJWT({ ...claims, run_id: claimMarker })
    .setProtectedHeader({ alg: 'RS256', kid })
    .setIssuedAt()
  if (iss) jwt.setIssuer(iss)
  if (aud !== null) jwt.setAudience(aud)
  if (exp !== null) jwt.setExpirationTime(exp)
  if (nbf !== undefined) jwt.setNotBefore(nbf)
  return jwt.sign(key)
}

function claimsFor(repoId: string, scopes = [{ Scope: scopeMarker, Permission: 3 }]): Claims {
  return { ac: JSON.stringify(scopes), repository_id: repoId }
}

async function twirp(method: string, token: string | undefined, body: object) {
  const res = await fetch(`${TWIRP}/${method}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token && { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(body),
  })
  const text = await res.text()
  let json: Record<string, unknown> = {}
  try {
    json = JSON.parse(text)
  } catch {}
  return { status: res.status, json }
}

async function save(token: string, key: string, data: Buffer) {
  const created = await twirp('CreateCacheEntry', token, { key, version: 'v1' })
  expect(created.status).toBe(200)
  const uploadUrl = created.json.signed_upload_url as string
  const part = await fetch(uploadUrl, { method: 'PUT', body: new Uint8Array(data) })
  expect(part.status).toBe(201)
  // The signed URL already has a query (the upload capability), so append to it.
  const blocklistUrl = new URL(uploadUrl)
  blocklistUrl.searchParams.set('comp', 'blocklist')
  const blocklist = await fetch(blocklistUrl, { method: 'PUT' })
  expect(blocklist.status).toBe(201)
  const finalized = await twirp('FinalizeCacheEntryUpload', token, {
    key,
    version: 'v1',
    size_bytes: data.length,
  })
  expect(finalized.status).toBe(200)
}

function lookup(token: string | undefined, key: string) {
  return twirp('GetCacheEntryDownloadURL', token, { key, version: 'v1' })
}

function parseEnvBase(input: object) {
  return envBaseSchema({ API_BASE_URL: 'http://localhost', ...input })
}

describe('direct downloads require token validation (config)', () => {
  const base = {
    ENABLE_DIRECT_DOWNLOADS: true,
    SKIP_TOKEN_VALIDATION: true,
    UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION: false,
  }

  test('refuses direct downloads with token validation skipped', () => {
    expect(() => assertDirectDownloadsRequireTokenValidation(base)).toThrow(
      'requires token validation',
    )
  })

  test('allows the combination only with the explicit unsafe override', () => {
    expect(() =>
      assertDirectDownloadsRequireTokenValidation({
        ...base,
        UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION: true,
      }),
    ).not.toThrow()
    expect(() =>
      assertDirectDownloadsRequireTokenValidation({ ...base, SKIP_TOKEN_VALIDATION: false }),
    ).not.toThrow()
  })

  test('direct downloads are off by default; the URL TTL defaults to 300 s within 1–3600', () => {
    expect(parseEnvBase({})).toMatchObject({
      ENABLE_DIRECT_DOWNLOADS: false,
      SKIP_TOKEN_VALIDATION: false,
      UNSAFE_ALLOW_DIRECT_DOWNLOADS_WITHOUT_TOKEN_VALIDATION: false,
      DIRECT_DOWNLOAD_URL_TTL_SECONDS: 300,
    })
    for (const ttl of [1, 3600])
      expect(parseEnvBase({ DIRECT_DOWNLOAD_URL_TTL_SECONDS: ttl })).toMatchObject({
        DIRECT_DOWNLOAD_URL_TTL_SECONDS: ttl,
      })
    for (const ttl of [0, 3601, 1.5])
      expect(parseEnvBase({ DIRECT_DOWNLOAD_URL_TTL_SECONDS: ttl })).toBeInstanceOf(type.errors)
  })

  test('the server refuses to start', { timeout: 30_000 }, async () => {
    const server = startServer({ ENABLE_DIRECT_DOWNLOADS: 'true', SKIP_TOKEN_VALIDATION: 'true' })
    if ((await readyOrExited(server)) === 'ready') {
      server.kill()
      throw new Error('server started with direct downloads and skipped token validation')
    }
    const result = await server
    expect(result.exitCode).not.toBe(0)
    expect(result.all).toContain('ENABLE_DIRECT_DOWNLOADS=true requires token validation')
  })
})

describe('token audience (config)', () => {
  test('unset skips the audience check; a list is trimmed', () => {
    expect(parseActionsTokenAudience(undefined)).toBeUndefined()
    expect(parseActionsTokenAudience(' a, b ,')).toEqual(['a', 'b'])
  })

  test.each(['', ' ', ',', ' , ,'])('refuses %j, which lists no audience', (value) => {
    expect(() => parseActionsTokenAudience(value)).toThrow('lists no audience')
  })

  test('the server refuses to start with an empty audience list', { timeout: 30_000 }, async () => {
    const server = startServer({ ACTIONS_TOKEN_AUDIENCE: ' , ' })
    if ((await readyOrExited(server)) === 'ready') {
      server.kill()
      throw new Error('server started with an empty ACTIONS_TOKEN_AUDIENCE')
    }
    const result = await server
    expect(result.exitCode).not.toBe(0)
    expect(result.all).toContain('ACTIONS_TOKEN_AUDIENCE is set but lists no audience')
  })
})

describe('token validation and direct downloads', () => {
  let server: ServerProcess
  let logs = ''
  const issuedTokens: string[] = []
  const track = async (token: Promise<string>) => {
    const value = await token
    issuedTokens.push(value)
    return value
  }

  beforeAll(async () => {
    await startIssuer()
    server = startServer({
      SKIP_TOKEN_VALIDATION: 'false',
      ACTIONS_TOKEN_ISSUER: issuer.url,
      ACTIONS_TOKEN_AUDIENCE: `other-audience, ${AUDIENCE}`,
      ENABLE_DIRECT_DOWNLOADS: 'true',
      EAGER_MERGE: 'true',
      DIRECT_DOWNLOAD_URL_TTL_SECONDS: String(URL_TTL_SECONDS),
    })
    server.all?.on('data', (chunk: Buffer) => {
      logs += chunk.toString()
    })
    if ((await readyOrExited(server)) !== 'ready')
      throw new Error(`server failed to start:\n${logs}`)
  }, 60_000)

  afterAll(async () => {
    server?.kill()
    await server
    const closed = Promise.withResolvers<void>()
    issuer.server?.close(() => closed.resolve())
    await closed.promise
  })

  test('accepts a token signed by the issuer', async () => {
    const token = await track(mint(claimsFor(repoA)))
    const res = await lookup(token, randomUUID())
    expect(res.status).toBe(200)
    expect(res.json.ok).toBe(false)
  })

  test.each([
    ['missing bearer', async () => undefined],
    [
      'signature from an untrusted key',
      () => mint(claimsFor(repoA), { key: untrusted.privateKey }),
    ],
    ['expired', () => mint(claimsFor(repoA), { exp: Math.floor(Date.now() / 1000) - 60 })],
    ['not yet valid', () => mint(claimsFor(repoA), { nbf: Math.floor(Date.now() / 1000) + 600 })],
    ['wrong issuer', () => mint(claimsFor(repoA), { iss: 'https://issuer.invalid' })],
    ['no expiry', () => mint(claimsFor(repoA), { exp: null })],
    ['wrong audience', () => mint(claimsFor(repoA), { aud: 'someone-else' })],
    ['no audience', () => mint(claimsFor(repoA), { aud: null })],
    ['no cache scopes', () => mint({ repository_id: repoA })],
    [
      'no repository id',
      () => mint({ ac: JSON.stringify([{ Scope: scopeMarker, Permission: 3 }]) }),
    ],
    ['malformed cache scopes', () => mint({ ac: '[{"Scope":1}]', repository_id: repoA })],
    [
      'unsigned token',
      async () => {
        const claims = {
          ...claimsFor(repoA),
          iss: issuer.url,
          exp: Math.floor(Date.now() / 1000) + 600,
        }
        return `${encodeSegment({ alg: 'none' })}.${encodeSegment(claims)}.`
      },
    ],
  ])('rejects %s with 401 on lookup, create and finalize', async (_, makeToken) => {
    const token = await makeToken()
    if (token) issuedTokens.push(token)
    const key = randomUUID()
    const res = await lookup(token, key)
    expect(res.status).toBe(401)
    expect(res.json.signed_download_url).toBeUndefined()

    const create = await twirp('CreateCacheEntry', token, { key, version: 'v1' })
    expect(create.status).toBe(401)
    expect(create.json.signed_upload_url).toBeUndefined()
    const finalize = await twirp('FinalizeCacheEntryUpload', token, { key, version: 'v1' })
    expect(finalize.status).toBe(401)
  })

  test(
    'a forged token gets no URL for an existing entry, direct or proxied',
    { timeout: 30_000 },
    async () => {
      const key = `forged-${randomUUID()}`
      const owner = await track(mint(claimsFor(repoA)))
      await save(owner, key, randomBytes(1024))

      const forged = await mint(claimsFor(repoA), { key: untrusted.privateKey })
      issuedTokens.push(forged)
      const res = await lookup(forged, key)
      expect(res.status).toBe(401)
      expect(JSON.stringify(res.json)).not.toContain('/download/')
      expect(JSON.stringify(res.json)).not.toContain('X-Amz-')
    },
  )

  test(
    'another repository cannot match the entry, even by prefix',
    { timeout: 30_000 },
    async () => {
      const key = `cross-repo-${randomUUID()}`
      await save(await track(mint(claimsFor(repoA))), key, randomBytes(1024))

      const other = await track(mint(claimsFor(repoB)))
      const exact = await lookup(other, key)
      expect(exact.json.ok).toBe(false)
      const prefix = await twirp('GetCacheEntryDownloadURL', other, {
        key: 'no-such-key',
        restore_keys: [key.slice(0, 12)],
        version: 'v1',
      })
      expect(prefix.json.ok).toBe(false)

      const owner = await lookup(await track(mint(claimsFor(repoA))), key)
      expect(owner.json.ok).toBe(true)
    },
  )

  test('scope permissions are bit flags', { timeout: 30_000 }, async () => {
    const key = `scopes-${randomUUID()}`
    await save(await track(mint(claimsFor(repoA))), key, randomBytes(1024))

    const readOnly = await track(mint(claimsFor(repoA, [{ Scope: scopeMarker, Permission: 1 }])))
    const read = await lookup(readOnly, key)
    expect(read.json.ok).toBe(true)
    const write = await twirp('CreateCacheEntry', readOnly, { key: randomUUID(), version: 'v1' })
    expect(write.status).toBe(403)

    const writeOnly = await track(mint(claimsFor(repoA, [{ Scope: scopeMarker, Permission: 2 }])))
    const writeOnlyRead = await lookup(writeOnly, key)
    expect(writeOnlyRead.status).toBe(403)

    const otherScope = await track(
      mint(claimsFor(repoA, [{ Scope: `refs/heads/other-${randomUUID()}`, Permission: 3 }])),
    )
    const otherScopeRead = await lookup(otherScope, key)
    expect(otherScopeRead.json.ok).toBe(false)
  })

  test(
    'a fork pull request writes only to its own ref and reads the base scopes it lists',
    { timeout: 30_000 },
    async () => {
      const baseRef = `refs/heads/base-${randomUUID()}`
      const prRef = `refs/pull/${randomUUID()}/merge`
      const baseKey = `base-${randomUUID()}`
      await save(
        await track(mint(claimsFor(repoA, [{ Scope: baseRef, Permission: 3 }]))),
        baseKey,
        randomBytes(1024),
      )

      // the read-only base scope is listed first; writes must still land on the PR ref
      const pr = await track(
        mint(
          claimsFor(repoA, [
            { Scope: baseRef, Permission: 1 },
            { Scope: prRef, Permission: 3 },
          ]),
        ),
      )
      const baseFromPr = await lookup(pr, baseKey)
      expect(baseFromPr.json.ok).toBe(true)

      const prKey = `pr-${randomUUID()}`
      await save(pr, prKey, randomBytes(1024))
      const prFromPr = await lookup(pr, prKey)
      expect(prFromPr.json.ok).toBe(true)
      const baseOnly = await track(mint(claimsFor(repoA, [{ Scope: baseRef, Permission: 3 }])))
      const prFromBase = await lookup(baseOnly, prKey)
      expect(prFromBase.json.ok).toBe(false)

      // a read-only token cannot finalize an upload either
      const readOnly = await track(mint(claimsFor(repoA, [{ Scope: prRef, Permission: 1 }])))
      const finalize = await twirp('FinalizeCacheEntryUpload', readOnly, {
        key: prKey,
        version: 'v1',
        size_bytes: 1024,
      })
      expect(finalize.status).toBe(403)
    },
  )

  test('accepts a key rotated in by the issuer (new kid)', { timeout: 60_000 }, async () => {
    const rotated = generateKeyPairSync('rsa', { modulusLength: 2048 })
    const rotatedKid = 'rotated-key'
    const token = await track(mint(claimsFor(repoA), { key: rotated.privateKey, kid: rotatedKid }))
    issuer.keys.push(await publicJwk(rotated.publicKey, rotatedKid))

    // A real wait: the server process refetches the JWKS for an unknown kid at
    // most once per 30 s (jose's cooldown), and fake timers can't reach into it.
    const fetches = issuer.jwksFetchedAt.length
    expect(fetches).toBeGreaterThan(0)
    await sleep(Math.max(0, issuer.jwksFetchedAt.at(-1)! + 31_000 - Date.now()))

    const res = await lookup(token, randomUUID())
    expect(res.status).toBe(200)
    expect(issuer.jwksFetchedAt.length).toBeGreaterThan(fetches)
  })

  test('fails closed with 401 when the JWKS cannot be fetched', { timeout: 30_000 }, async () => {
    const port = PORT + 1
    const broken = startServer({
      NITRO_PORT: String(port),
      API_BASE_URL: `http://localhost:${port}`,
      SKIP_TOKEN_VALIDATION: 'false',
      ACTIONS_TOKEN_ISSUER: issuer.url,
      ACTIONS_TOKEN_JWKS_URL: `${issuer.url}/broken-jwks`,
    })
    let brokenLogs = ''
    broken.all?.on('data', (chunk: Buffer) => {
      brokenLogs += chunk.toString()
    })
    try {
      if ((await readyOrExited(broken)) !== 'ready')
        throw new Error(`server failed to start:\n${brokenLogs}`)
      const token = await track(mint(claimsFor(repoA)))
      const res = await fetch(
        `http://localhost:${port}/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'authorization': `Bearer ${token}` },
          body: JSON.stringify({ key: randomUUID(), version: 'v1' }),
        },
      )
      expect(res.status).toBe(401)
      expect(brokenLogs).not.toContain(token)
      expect(brokenLogs).not.toContain(claimMarker)
    } finally {
      broken.kill()
      await broken
    }
  })

  test(
    'an authorized lookup of a merged entry gets a URL that expires within the TTL',
    { timeout: 60_000 },
    async () => {
      const key = `direct-${randomUUID()}`
      const data = randomBytes(64 * 1024)
      const token = await track(mint(claimsFor(repoA)))
      await save(token, key, data)

      const signedUrl = async () => {
        const res = await lookup(token, key)
        expect(res.json.ok).toBe(true)
        return new URL(res.json.signed_download_url as string)
      }

      if (process.env.STORAGE_DRIVER === 'filesystem') {
        // No signed URLs on the filesystem driver: always the proxied route.
        const url = await signedUrl()
        expect(url.origin).toBe(BASE_URL)
        expect(url.pathname).toMatch(/^\/download\//)
        return
      }

      // Eager Merge runs after finalize; until it lands the lookup is proxied.
      const url = await vi.waitFor(
        async () => {
          const candidate = await signedUrl()
          expect(candidate.origin).not.toBe(BASE_URL)
          return candidate
        },
        { timeout: 30_000, interval: 200 },
      )
      // S3 signs a relative lifetime; GCS (V2 signing) an absolute epoch second
      const gcsExpiresAt = url.searchParams.get('Expires')
      const expires = gcsExpiresAt
        ? Number(gcsExpiresAt) - Math.floor(Date.now() / 1000)
        : Number(url.searchParams.get('X-Amz-Expires'))
      expect(expires).toBeGreaterThan(0)
      expect(expires).toBeLessThanOrEqual(URL_TTL_SECONDS)

      const download = await fetch(url)
      const body = Buffer.from(await download.arrayBuffer())
      expect(body.equals(data)).toBe(true)
    },
  )

  test('logs contain neither tokens, claims nor signed URLs', () => {
    expect(issuedTokens.length).toBeGreaterThan(0)
    for (const token of issuedTokens) {
      expect(logs).not.toContain(token)
      // nor its signature on its own
      const signature = token.slice(token.lastIndexOf('.') + 1)
      if (signature) expect(logs).not.toContain(signature)
    }
    // Repository ids, scopes and keys do appear in DEBUG query logs; the claims
    // jose decodes from a rejected token must not.
    expect(logs).not.toContain(claimMarker)
    expect(logs).not.toContain('X-Amz-Signature')
    expect(logs).not.toContain('GoogleAccessId=')
    // rejections are still diagnosable
    expect(logs).toContain('ERR_JWT_EXPIRED')
  })
})

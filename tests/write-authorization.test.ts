import { Buffer } from 'node:buffer'
import crypto from 'node:crypto'

import { SignJWT } from 'jose'
import { describe, expect, test } from 'vitest'

const TWIRP_BASE = 'http://localhost:3000/twirp/github.actions.results.api.v1.CacheService'

function signToken({
  repoId = '123',
  scopes = [{ Scope: 'refs/heads/main', Permission: 3 }],
}: { repoId?: string; scopes?: { Scope: string; Permission: number }[] } = {}) {
  return new SignJWT({ ac: JSON.stringify(scopes), repository_id: repoId })
    .setProtectedHeader({ alg: 'HS256' })
    .sign(crypto.createSecretKey('mock-secret-key', 'ascii'))
}

async function twirp<T>(method: string, token: string, body: object) {
  const response = await fetch(`${TWIRP_BASE}/${method}`, {
    method: 'POST',
    headers: { 'authorization': `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  return { status: response.status, body: (await response.json()) as T }
}

async function createUpload(token: string, key: string) {
  const { status, body } = await twirp<{ ok: boolean; signed_upload_url: string }>(
    'CreateCacheEntry',
    token,
    { key, version: 'v1' },
  )
  expect(status).toBe(200)
  expect(body.ok).toBe(true)
  return body.signed_upload_url
}

async function save(token: string, key: string, data: Uint8Array) {
  const url = await createUpload(token, key)
  const put = await fetch(url, { method: 'PUT', body: data as BodyInit })
  expect(put.status).toBe(201)
  const { status } = await twirp('FinalizeCacheEntryUpload', token, { key, version: 'v1' })
  expect(status).toBe(200)
}

async function restore(token: string, key: string) {
  const { body } = await twirp<{ ok: boolean; signed_download_url?: string }>(
    'GetCacheEntryDownloadURL',
    token,
    { key, version: 'v1' },
  )
  if (!body.ok || !body.signed_download_url) return
  const response = await fetch(body.signed_download_url)
  expect(response.status).toBe(200)
  return Buffer.from(await response.arrayBuffer())
}

describe('write path authorization', () => {
  test('another repository cannot replace or read an entry with the same key and scope', async () => {
    const tokenA = await signToken({ repoId: 'write-auth-owner' })
    const tokenB = await signToken({ repoId: 'write-auth-other' })
    const key = `write-auth-${crypto.randomUUID()}`
    const original = crypto.randomBytes(64)

    await save(tokenA, key, original)
    expect(await restore(tokenB, key)).toBeUndefined()

    await save(tokenB, key, crypto.randomBytes(64))
    const restored = await restore(tokenA, key)
    expect(restored?.equals(original)).toBe(true)
  })

  test('a pull request ref write scope cannot replace a base branch entry', async () => {
    const base = await signToken({ repoId: 'write-auth-fork' })
    const pullRequest = await signToken({
      repoId: 'write-auth-fork',
      scopes: [
        { Scope: 'refs/pull/1/merge', Permission: 3 },
        { Scope: 'refs/heads/main', Permission: 1 },
      ],
    })
    const key = `write-auth-${crypto.randomUUID()}`
    const original = crypto.randomBytes(64)
    await save(base, key, original)

    // the pull request restores the base entry but saves into its own ref
    const restored = await restore(pullRequest, key)
    expect(restored?.equals(original)).toBe(true)
    await save(pullRequest, key, crypto.randomBytes(64))
    const baseRestored = await restore(base, key)
    expect(baseRestored?.equals(original)).toBe(true)
  })
})

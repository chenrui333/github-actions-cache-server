import { Buffer } from 'node:buffer'
import crypto from 'node:crypto'

import { SignJWT } from 'jose'
import { beforeAll, describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import { reservationHash } from '~/lib/helpers'

const BASE = 'http://localhost:3000'
const TWIRP_BASE = `${BASE}/twirp/github.actions.results.api.v1.CacheService`

function signToken({ repoId = '123', scope = 'refs/heads/main' } = {}) {
  return new SignJWT({
    ac: JSON.stringify([{ Scope: scope, Permission: 3 }]),
    repository_id: repoId,
  })
    .setProtectedHeader({ alg: 'HS256' })
    .sign(crypto.createSecretKey('mock-secret-key', 'ascii'))
}

async function twirp<T>(method: string, token: string, body: object) {
  const response = await fetch(`${TWIRP_BASE}/${method}`, {
    method: 'POST',
    headers: { 'authorization': `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const json = (await response.json()) as T
  return { status: response.status, body: json }
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

/** Block id in the 48-byte format (uuid + zero-padded index) used by `@actions/cache`. */
function blockId(index: number) {
  const padded =
    index < 0 ? `-${String(-index).padStart(11, '0')}` : String(index).padStart(12, '0')
  return Buffer.from(`${crypto.randomUUID()}${padded}`).toString('base64')
}

/** Block id in the 64-byte format used by docker buildx (index at byte 16). */
function buildxBlockId(index: number) {
  const id = crypto.randomBytes(64)
  id.writeUInt32BE(index, 16)
  return id.toString('base64')
}

/**
 * Inserts an Upload as it was created before Upload URLs carried a capability,
 * and returns the URL it was issued (with a made-up capability appended).
 */
async function createUploadWithoutCapability(key: string) {
  const reservation = { key, version: 'v1', scope: 'refs/heads/main', repoId: '123' }
  const id = crypto.randomInt(1, 2 ** 31)
  const db = await getDatabase()
  await db
    .insertInto('uploads')
    .values({
      ...reservation,
      id,
      folderName: id.toString(),
      createdAt: Date.now(),
      lastPartUploadedAt: null,
      finishedPartUploadCount: 0,
      startedPartUploadCount: 0,
      reservationHash: reservationHash(reservation),
      capabilityHash: null,
    })
    .execute()
  return `${BASE}/devstoreaccount1/upload/${id}?sig=${crypto.randomBytes(32).toString('hex')}`
}

/** Appends Azure Blob-style query parameters the way the client SDKs do. */
function withParams(url: string, params: Record<string, string>) {
  const parsed = new URL(url)
  for (const [name, value] of Object.entries(params)) parsed.searchParams.set(name, value)
  return parsed.href
}

function withoutQuery(url: string) {
  const parsed = new URL(url)
  parsed.search = ''
  return parsed.href
}

/** PUTs `body` and returns the response status and error message, if any. */
async function put(url: string, body?: Uint8Array) {
  const response = await fetch(url, { method: 'PUT', body: body as BodyInit | undefined })
  const text = await response.text()
  const message = response.ok ? undefined : (JSON.parse(text) as { statusMessage: string })
  return { status: response.status, statusMessage: message?.statusMessage }
}

async function finalize(token: string, key: string) {
  const { status } = await twirp<{ ok: boolean }>('FinalizeCacheEntryUpload', token, {
    key,
    version: 'v1',
  })
  return status
}

async function downloadEntry(token: string, key: string) {
  const { body } = await twirp<{ ok: boolean; signed_download_url: string }>(
    'GetCacheEntryDownloadURL',
    token,
    { key, version: 'v1' },
  )
  expect(body.ok).toBe(true)
  const response = await fetch(body.signed_download_url)
  expect(response.status).toBe(200)
  return Buffer.from(await response.arrayBuffer())
}

describe('upload URL authorization', () => {
  let token: string
  beforeAll(async () => {
    token = await signToken()
  })

  test('an upload URL without its capability cannot add data to an in-flight upload', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUpload(token, key)
    const expected = crypto.randomBytes(1024)

    expect(await put(url, expected)).toMatchObject({ status: 201 })
    for (const index of [-1, 1]) {
      const foreign = await put(
        withParams(withoutQuery(url), { comp: 'block', blockid: blockId(index) }),
        crypto.randomBytes(512),
      )
      expect(foreign.status).toBe(403)
    }

    expect(await finalize(token, key)).toBe(200)
    const downloaded = await downloadEntry(token, key)
    expect(downloaded.equals(expected)).toBe(true)
  })

  test('a wrong capability and an unknown upload get the same rejection', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUpload(token, key)
    const forged = crypto.randomBytes(32).toString('hex')

    const rejected = [
      await put(withParams(url, { sig: forged }), crypto.randomBytes(16)),
      await put(
        withParams(url.replace(/\/upload\/\d+/, '/upload/1'), { sig: forged }),
        crypto.randomBytes(16),
      ),
      await put(withParams(withoutQuery(url), { comp: 'blocklist' })),
    ]
    for (const response of rejected)
      expect(response).toEqual({ status: 403, statusMessage: 'Forbidden' })

    // The rejected requests left no parts behind: the owner's upload still finalizes.
    const expected = crypto.randomBytes(64)
    expect(await put(url, expected)).toMatchObject({ status: 201 })
    expect(await finalize(token, key)).toBe(200)
    const downloaded = await downloadEntry(token, key)
    expect(downloaded.equals(expected)).toBe(true)
  })

  test('chunked block and blocklist uploads work with the capability', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUpload(token, key)
    const chunks = [crypto.randomBytes(2048), crypto.randomBytes(1024)]

    for (const [index, chunk] of chunks.entries()) {
      const response = await put(withParams(url, { comp: 'block', blockid: blockId(index) }), chunk)
      expect(response.status).toBe(201)
    }
    expect(await put(withParams(url, { comp: 'blocklist' }))).toMatchObject({ status: 201 })

    expect(await finalize(token, key)).toBe(200)
    const downloaded = await downloadEntry(token, key)
    expect(downloaded.equals(Buffer.concat(chunks))).toBe(true)
  })

  test('only the creating repository and scope can finalize an upload', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUpload(token, key)
    const expected = crypto.randomBytes(256)
    expect(await put(url, expected)).toMatchObject({ status: 201 })

    const otherRepo = await signToken({ repoId: '456' })
    const otherScope = await signToken({ scope: 'refs/heads/other' })
    expect(await finalize(otherRepo, key)).toBe(404)
    expect(await finalize(otherScope, key)).toBe(404)

    expect(await finalize(token, key)).toBe(200)
    const downloaded = await downloadEntry(token, key)
    expect(downloaded.equals(expected)).toBe(true)
  })

  test('chunked uploads with docker buildx block ids work with the capability', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUpload(token, key)
    const chunks = [crypto.randomBytes(1024), crypto.randomBytes(512), crypto.randomBytes(256)]

    for (const [index, chunk] of chunks.entries()) {
      const block = withParams(url, { comp: 'block', blockid: buildxBlockId(index) })
      expect(await put(block, chunk)).toMatchObject({ status: 201 })
      expect(await put(withoutQuery(block), chunk)).toMatchObject({ status: 403 })
    }
    expect(await put(withParams(url, { comp: 'blocklist' }))).toMatchObject({ status: 201 })

    expect(await finalize(token, key)).toBe(200)
    const downloaded = await downloadEntry(token, key)
    expect(downloaded.equals(Buffer.concat(chunks))).toBe(true)
  })

  test('a capability only authorizes its own upload', async () => {
    const otherRepo = await signToken({ repoId: '456' })
    const key = `upload-auth-${crypto.randomUUID()}`
    const ours = new URL(await createUpload(token, key))
    const theirs = new URL(await createUpload(otherRepo, key))

    // our capability on their upload id
    theirs.searchParams.set('sig', ours.searchParams.get('sig')!)
    expect(await put(theirs.href, crypto.randomBytes(16))).toEqual({
      status: 403,
      statusMessage: 'Forbidden',
    })
  })

  test('an upload created without a capability accepts no parts', async () => {
    const key = `upload-auth-${crypto.randomUUID()}`
    const url = await createUploadWithoutCapability(key)

    for (const target of [url, withoutQuery(url), withParams(url, { comp: 'blocklist' })])
      expect(await put(target, crypto.randomBytes(16))).toEqual({
        status: 403,
        statusMessage: 'Forbidden',
      })
  })
})

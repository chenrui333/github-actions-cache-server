import type { Kysely } from 'kysely'
import type { Database } from '~/lib/db'
import type { Storage } from '~/lib/storage'
import { Buffer } from 'node:buffer'
import { createHash, randomUUID } from 'node:crypto'
import { once } from 'node:events'
import net from 'node:net'
import { PassThrough } from 'node:stream'
import { SignJWT } from 'jose'
import { describe, expect, test, vi } from 'vitest'
import { z } from 'zod'
import { getDatabase } from '~/lib/db'
import { getStorage } from '~/lib/storage'
import { deleteUploadIfIdle } from '~/lib/storage-lifecycle'

const API = 'http://localhost:3000/twirp/github.actions.results.api.v1.CacheService'
const createdSchema = z.object({ ok: z.literal(true), signed_upload_url: z.url() })
const restoredSchema = z.object({ ok: z.literal(true), signed_download_url: z.url() })
const creationOutcomeSchema = z.object({ ok: z.boolean() })
interface Fixture {
  db: Kysely<Database>
  storage: Storage
  identity: { key: string; version: string; scope: string; repoId: string }
  token: string
  url: URL
  id: number
  capability: string
}

async function responseStatus(response: Promise<Response>) {
  const result = await response
  return result.status
}

async function creationOk(response: Response) {
  const result = creationOutcomeSchema.parse(await response.json())
  return result.ok
}

async function token(repoId: string, scope: string) {
  return new SignJWT({
    repository_id: repoId,
    ac: JSON.stringify([{ Scope: scope, Permission: 3 }]),
    jti: randomUUID(),
  })
    .setProtectedHeader({ alg: 'HS256' })
    .sign(Buffer.from('fixture-only-not-a-production-key'))
}

function rpc(method: string, bearer: string, body: object) {
  return fetch(`${API}/${method}`, {
    method: 'POST',
    headers: { 'authorization': `Bearer ${bearer}`, 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
}

async function fixture(key = randomUUID(), repoId = 'completion-tests', scope = 'refs/heads/main') {
  const identity = { key, version: 'v1', scope, repoId }
  const bearer = await token(repoId, scope)
  const response = await rpc('CreateCacheEntry', bearer, identity)
  expect(response.status).toBe(200)
  const created = createdSchema.parse(await response.json())
  expect(created.ok).toBe(true)
  const url = new URL(created.signed_upload_url)
  return {
    db: await getDatabase(),
    storage: await getStorage(),
    identity,
    token: bearer,
    url,
    id: Number(url.pathname.split('/').at(-1)),
    capability: url.searchParams.get('sig')!,
  } satisfies Fixture
}

function blockId(value: number) {
  return Buffer.alloc(16, value).toString('base64')
}

function blockUrl(state: Fixture, id: string) {
  const url = new URL(state.url)
  url.searchParams.set('comp', 'block')
  url.searchParams.set('blockid', id)
  return url
}

function commit(state: Fixture, ids: string[], xml?: string) {
  const url = new URL(state.url)
  url.searchParams.set('comp', 'blocklist')
  return fetch(url, {
    method: 'PUT',
    body: xml ?? `<BlockList>${ids.map((id) => `<Latest>${id}</Latest>`).join('')}</BlockList>`,
  })
}

async function restored(state: Fixture) {
  const response = await rpc('GetCacheEntryDownloadURL', state.token, state.identity)
  const result = restoredSchema.parse(await response.json())
  expect(result.ok).toBe(true)
  const download = await fetch(result.signed_download_url)
  expect(download.status).toBe(200)
  return Buffer.from(await download.arrayBuffer())
}

async function restoredText(state: Fixture) {
  const body = await restored(state)
  return body.toString()
}

async function assertPending(state: Fixture) {
  expect(
    await state.db.selectFrom('uploads').select('id').where('id', '=', state.id).executeTakeFirst(),
  ).toBeDefined()
  expect(
    await state.db
      .selectFrom('cache_entries')
      .select('id')
      .where('key', '=', state.identity.key)
      .where('scope', '=', state.identity.scope)
      .where('repoId', '=', state.identity.repoId)
      .execute(),
  ).toHaveLength(0)
}

async function makeLegacy(
  state: Fixture,
  changes: {
    createdAt?: number
    lastPartUploadedAt?: number | null
    startedPartUploadCount?: number
  } = {},
) {
  await state.db
    .updateTable('uploads')
    .set({
      capabilityHash: null,
      createdAt: Date.now() - 25 * 86_400_000,
      lastPartUploadedAt: Date.now() - 25 * 86_400_000,
      ...changes,
    })
    .where('id', '=', state.id)
    .execute()
}

describe('authenticated complete-payload publication', () => {
  test('whole PUT publishes exact bytes, read-only acknowledgment, cold/warm restore and idempotent retry', async () => {
    const state = await fixture()
    expect(await responseStatus(fetch(state.url, { method: 'PUT', body: 'whole-payload' }))).toBe(
      201,
    )
    expect(await restoredText(state)).toBe('whole-payload')
    await state.storage.waitForOngoingMerges()
    expect(await restoredText(state)).toBe('whole-payload')
    const before = await state.db
      .selectFrom('cache_entries')
      .selectAll()
      .where('key', '=', state.identity.key)
      .execute()
    expect(
      await responseStatus(
        rpc('FinalizeCacheEntryUpload', state.token, { ...state.identity, size_bytes: 13 }),
      ),
    ).toBe(200)
    expect(
      await responseStatus(
        rpc('FinalizeCacheEntryUpload', state.token, { ...state.identity, size_bytes: 2 }),
      ),
    ).toBe(409)
    expect(await responseStatus(fetch(state.url, { method: 'PUT', body: 'whole-payload' }))).toBe(
      201,
    )
    expect(await responseStatus(fetch(state.url, { method: 'PUT', body: 'replacement' }))).toBe(409)
    expect(
      await responseStatus(fetch(blockUrl(state, blockId(1)), { method: 'PUT', body: 'late' })),
    ).toBe(409)
    expect(
      await state.db
        .selectFrom('cache_entries')
        .selectAll()
        .where('key', '=', state.identity.key)
        .execute(),
    ).toEqual(before)
    expect(
      await state.db
        .selectFrom('upload_receipts')
        .select('uploadId')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(1)
  })

  test('opaque 16-byte IDs, manifest order, repeated references, latest revisions and omitted extra blocks', async () => {
    const state = await fixture()
    const a = blockId(2)
    const b = blockId(1)
    const extra = blockId(3)
    for (const [id, body] of [
      [b, 'B'],
      [a, 'old-A'],
      [extra, 'EXCLUDED'],
      [a, 'A'],
    ])
      expect(await responseStatus(fetch(blockUrl(state, id), { method: 'PUT', body }))).toBe(201)
    expect(await responseStatus(commit(state, [a, b, a]))).toBe(201)
    expect(await restoredText(state)).toBe('ABA')
    await state.storage.waitForOngoingMerges()
    expect(await restoredText(state)).toBe('ABA')
    expect(await responseStatus(commit(state, [a, b, a]))).toBe(201)
    expect(await responseStatus(commit(state, [b, a]))).toBe(409)
  })

  test('missing blocks, malformed/DTD manifests and mixed duplicate sources cannot publish or discard pending data', async () => {
    const state = await fixture()
    const id = blockId(4)
    expect(await responseStatus(fetch(blockUrl(state, id), { method: 'PUT', body: 'kept' }))).toBe(
      201,
    )
    expect(await responseStatus(commit(state, [id, blockId(5)]))).toBe(400)
    for (const xml of [
      '<not-a-block-list/>',
      '<BlockList><Latest>',
      '<!DOCTYPE x><BlockList/>',
      `<BlockList><Latest>${id}</Latest><Uncommitted>${id}</Uncommitted></BlockList>`,
      `<BlockList><Latest>invalid</Latest></BlockList>`,
    ]) {
      expect(await responseStatus(commit(state, [], xml))).toBe(400)
      await assertPending(state)
    }
    expect(await responseStatus(commit(state, [id]))).toBe(201)
    expect(await restoredText(state)).toBe('kept')
  })

  test('an empty authenticated payload is a real zero-byte cache, not a dangling entry', async () => {
    for (const mode of ['whole', 'blocks']) {
      const state = await fixture()
      const response =
        mode === 'whole'
          ? await fetch(state.url, { method: 'PUT', body: '' })
          : await commit(state, [])
      expect(response.status).toBe(201)
      expect(await restored(state)).toHaveLength(0)
    }
  })

  test('concurrent stage writes fence completion; delayed finalizers never publish a partial successor', async () => {
    const state = await fixture()
    const id = blockId(6)
    const stream = new PassThrough()
    const staged = state.storage.stageBlock(state.id, state.capability, id, stream)
    await vi.waitFor(async () => {
      expect(
        await state.db
          .selectFrom('upload_parts')
          .select('id')
          .where('uploadId', '=', state.id)
          .where('sizeBytes', 'is', null)
          .execute(),
      ).toHaveLength(1)
    })
    expect(await responseStatus(commit(state, [id]))).toBe(409)
    expect(await responseStatus(rpc('FinalizeCacheEntryUpload', state.token, state.identity))).toBe(
      404,
    )
    await assertPending(state)
    stream.end('complete-after-stream-end')
    await staged
    const responses = await Promise.all([commit(state, [id]), commit(state, [id])])
    expect(responses.map(({ status }) => status)).toEqual([201, 201])
    expect(await restoredText(state)).toBe('complete-after-stream-end')
    expect(
      await state.db
        .selectFrom('cache_entries')
        .select('id')
        .where('key', '=', state.identity.key)
        .execute(),
    ).toHaveLength(1)
    expect(
      await state.db
        .selectFrom('upload_receipts')
        .select('uploadId')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(1)
  })

  test('truncated HTTP bodies are never published', async () => {
    const state = await fixture()
    const socket = net.createConnection({ host: 'localhost', port: 3000 })
    await once(socket, 'connect')
    socket.end(
      `PUT ${state.url.pathname}${state.url.search} HTTP/1.1\r\nHost: localhost:3000\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort`,
    )
    socket.resume()
    await once(socket, 'close')
    await vi.waitFor(async () => {
      expect(
        await state.db
          .selectFrom('upload_parts')
          .select('id')
          .where('uploadId', '=', state.id)
          .execute(),
      ).toHaveLength(0)
    })
    await assertPending(state)
    expect(
      await state.db
        .selectFrom('upload_receipts')
        .select('uploadId')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(0)
  })

  test('wrong capability, ID, repository and scope cannot publish or acknowledge another generation', async () => {
    const state = await fixture()
    const other = await fixture()
    const copied = new URL(other.url)
    copied.searchParams.set('sig', state.capability)
    expect(await responseStatus(fetch(copied, { method: 'PUT', body: 'forged' }))).toBe(403)
    expect(await responseStatus(fetch(state.url, { method: 'PUT', body: 'owned' }))).toBe(201)
    for (const bearer of [
      await token('another-repo', state.identity.scope),
      await token(state.identity.repoId, 'refs/heads/another'),
    ])
      expect(await responseStatus(rpc('FinalizeCacheEntryUpload', bearer, state.identity))).toBe(
        404,
      )
    expect(await restoredText(state)).toBe('owned')
    await assertPending(other)
  })
})

describe('requested-key legacy recovery without global cleanup', () => {
  test('same-token legacy finalizers and old upload URLs cannot touch a successor, including between its parts', async () => {
    const state = await fixture()
    const oldUrl = new URL(state.url)
    const oldId = state.id
    await makeLegacy(state)
    const created = await rpc('CreateCacheEntry', state.token, state.identity)
    const result = createdSchema.parse(await created.json())
    expect(result.ok).toBe(true)
    state.url = new URL(result.signed_upload_url)
    state.id = Number(state.url.pathname.split('/').at(-1))
    state.capability = state.url.searchParams.get('sig')!
    expect(state.id).not.toBe(oldId)
    expect(await responseStatus(fetch(oldUrl, { method: 'PUT', body: 'old' }))).toBe(403)
    const before = await state.db
      .selectFrom('uploads')
      .selectAll()
      .where('id', '=', state.id)
      .executeTakeFirstOrThrow()
    expect(await responseStatus(rpc('FinalizeCacheEntryUpload', state.token, state.identity))).toBe(
      404,
    )
    expect(
      await state.db
        .selectFrom('uploads')
        .selectAll()
        .where('id', '=', state.id)
        .executeTakeFirst(),
    ).toEqual(before)
    const a = blockId(7)
    const b = blockId(8)
    expect(await responseStatus(fetch(blockUrl(state, a), { method: 'PUT', body: 'first' }))).toBe(
      201,
    )
    expect(await responseStatus(rpc('FinalizeCacheEntryUpload', state.token, state.identity))).toBe(
      404,
    )
    await assertPending(state)
    expect(await responseStatus(fetch(blockUrl(state, b), { method: 'PUT', body: 'second' }))).toBe(
      201,
    )
    expect(await responseStatus(commit(state, [a, b]))).toBe(201)
    const published = await state.db
      .selectFrom('cache_entries')
      .selectAll()
      .where('key', '=', state.identity.key)
      .execute()
    expect(await responseStatus(rpc('FinalizeCacheEntryUpload', state.token, state.identity))).toBe(
      200,
    )
    expect(
      await state.db
        .selectFrom('cache_entries')
        .selectAll()
        .where('key', '=', state.identity.key)
        .execute(),
    ).toEqual(published)
    expect(await restoredText(state)).toBe('firstsecond')
  })

  test('modern, recently active and unequal-counter legacy reservations remain untouched', async () => {
    for (const kind of ['modern', 'recent-created', 'recent-part', 'inflight']) {
      const state = await fixture()
      if (kind === 'modern')
        await state.db
          .updateTable('uploads')
          .set({ createdAt: 1, lastPartUploadedAt: 1 })
          .where('id', '=', state.id)
          .execute()
      else
        await makeLegacy(
          state,
          kind === 'recent-created'
            ? { createdAt: Date.now() }
            : kind === 'recent-part'
              ? { lastPartUploadedAt: Date.now() }
              : { startedPartUploadCount: 1 },
        )
      const before = await state.db
        .selectFrom('uploads')
        .selectAll()
        .where('id', '=', state.id)
        .executeTakeFirstOrThrow()
      const response = await rpc('CreateCacheEntry', state.token, state.identity)
      expect(await creationOk(response)).toBe(false)
      expect(
        await state.db
          .selectFrom('uploads')
          .selectAll()
          .where('id', '=', state.id)
          .executeTakeFirst(),
      ).toEqual(before)
    }
  })

  test('completed entries and unrelated scopes/repositories survive requested-key recovery', async () => {
    const state = await fixture()
    expect(await responseStatus(fetch(state.url, { method: 'PUT', body: 'completed' }))).toBe(201)
    const pending = await fixture(state.identity.key, state.identity.repoId, state.identity.scope)
    await makeLegacy(pending)
    const response = await rpc('CreateCacheEntry', state.token, state.identity)
    expect(await creationOk(response)).toBe(false)
    expect(await restoredText(state)).toBe('completed')
    const otherRepo = await fixture(state.identity.key, 'unrelated-repo')
    const otherScope = await fixture(
      state.identity.key,
      state.identity.repoId,
      'refs/heads/unrelated',
    )
    const unrelated = await state.db
      .selectFrom('uploads')
      .selectAll()
      .where('id', 'in', [otherRepo.id, otherScope.id])
      .execute()
    const recoverable = await fixture()
    await makeLegacy(recoverable)
    const outcomes = await Promise.all([
      rpc('CreateCacheEntry', recoverable.token, recoverable.identity),
      rpc('CreateCacheEntry', recoverable.token, recoverable.identity),
    ])
    const results = await Promise.all(outcomes.map(creationOk))
    expect(results.filter(Boolean)).toHaveLength(1)
    expect(
      await state.db
        .selectFrom('uploads')
        .selectAll()
        .where('id', 'in', [otherRepo.id, otherScope.id])
        .execute(),
    ).toEqual(unrelated)
  })
})

describe('generation integrity and retirement boundaries', () => {
  test.each([16, 48, 64])(
    'preserves opaque %i-byte client block IDs and manifest order',
    async (length) => {
      const state = await fixture()
      const ids = [0, 1].map((index) => {
        if (length === 48)
          return Buffer.from(`${randomUUID()}${String(index).padStart(12, '0')}`).toString('base64')
        const value = Buffer.alloc(length)
        Buffer.from(randomUUID().replaceAll('-', ''), 'hex').copy(value)
        if (length === 64) value.writeUInt32BE(index, 16)
        return value.toString('base64')
      })
      expect(
        await responseStatus(fetch(blockUrl(state, ids[0]!), { method: 'PUT', body: 'first' })),
      ).toBe(201)
      expect(
        await responseStatus(fetch(blockUrl(state, ids[1]!), { method: 'PUT', body: 'second' })),
      ).toBe(201)
      expect(await responseStatus(commit(state, [ids[1]!, ids[0]!]))).toBe(201)
      expect(await restoredText(state)).toBe('secondfirst')
    },
  )

  test('body checksum failure cannot publish and a legitimate retry remains usable', async () => {
    const state = await fixture()
    expect(
      await responseStatus(
        fetch(state.url, {
          method: 'PUT',
          body: 'payload',
          headers: { 'content-md5': Buffer.alloc(16).toString('base64') },
        }),
      ),
    ).toBe(400)
    await assertPending(state)
    expect(
      await state.db
        .selectFrom('upload_parts')
        .select('id')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(0)
    const checksum = createHash('md5').update('payload').digest('base64')
    expect(
      await responseStatus(
        fetch(state.url, {
          method: 'PUT',
          body: 'payload',
          headers: { 'content-md5': checksum },
        }),
      ),
    ).toBe(201)
    expect(await restoredText(state)).toBe('payload')
    expect(
      await state.db
        .selectFrom('upload_parts')
        .select('id')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(0)
  })

  test('failed and active streaming attempts are not completion proof or idle-cleanup targets', async () => {
    const state = await fixture()
    const ancient = Date.now() - 25 * 86_400_000
    await state.db.updateTable('uploads').set({ createdAt: ancient, lastPartUploadedAt: ancient })
      .where('id', '=', state.id).execute()
    const stream = new PassThrough()
    const attempt = state.storage.stageBlock(state.id, state.capability, blockId(12), stream)
    await vi.waitFor(async () => {
      expect(
        await state.db
          .selectFrom('upload_parts')
          .select('id')
          .where('uploadId', '=', state.id)
          .where('sizeBytes', 'is', null)
          .execute(),
      ).toHaveLength(1)
    })
    const removed = await state.db
      .transaction()
      .execute((tx) => deleteUploadIfIdle(tx, state.id, Date.now() + 1000))
    expect(removed).toBeUndefined()
    expect(await creationOk(await rpc('CreateCacheEntry', state.token, state.identity))).toBe(false)
    expect(await responseStatus(rpc('FinalizeCacheEntryUpload', state.token, state.identity))).toBe(
      404,
    )
    stream.destroy(new Error('interrupted fixture stream'))
    await expect(attempt).rejects.toThrow('interrupted fixture stream')
    expect(await responseStatus(commit(state, [blockId(12)]))).toBe(400)
    await assertPending(state)
    expect(
      await responseStatus(fetch(blockUrl(state, blockId(12)), { method: 'PUT', body: 'retry' })),
    ).toBe(201)
    expect(await responseStatus(commit(state, [blockId(12)]))).toBe(201)
    expect(await restoredText(state)).toBe('retry')
  })

  test('competing completion manifests publish only one immutable generation', async () => {
    const state = await fixture()
    const a = blockId(13)
    const b = blockId(14)
    expect(await responseStatus(fetch(blockUrl(state, a), { method: 'PUT', body: 'A' }))).toBe(201)
    expect(await responseStatus(fetch(blockUrl(state, b), { method: 'PUT', body: 'B' }))).toBe(201)
    const statuses = await Promise.all([
      responseStatus(commit(state, [a])),
      responseStatus(commit(state, [b])),
    ])
    expect(statuses.toSorted((left, right) => left - right)).toEqual([201, 409])
    expect(await restoredText(state)).toMatch(/^[AB]$/)
    expect(
      await state.db
        .selectFrom('cache_entries')
        .select('id')
        .where('key', '=', state.identity.key)
        .execute(),
    ).toHaveLength(1)
    expect(
      await state.db
        .selectFrom('upload_receipts')
        .select('uploadId')
        .where('uploadId', '=', state.id)
        .execute(),
    ).toHaveLength(1)
  })

  test('legacy retirement exceeds a five-day job and refuses ambiguous duplicate reservations', async () => {
    const recent = await fixture()
    const sixDaysAgo = Date.now() - 6 * 86_400_000
    await makeLegacy(recent, { createdAt: sixDaysAgo, lastPartUploadedAt: sixDaysAgo })
    const original = await recent.db
      .selectFrom('uploads')
      .selectAll()
      .where('id', '=', recent.id)
      .executeTakeFirstOrThrow()
    expect(await creationOk(await rpc('CreateCacheEntry', recent.token, recent.identity))).toBe(
      false,
    )
    expect(
      await recent.db
        .selectFrom('uploads')
        .selectAll()
        .where('id', '=', recent.id)
        .executeTakeFirst(),
    ).toEqual(original)
    const duplicate = await fixture()
    await makeLegacy(duplicate)
    const legacy = await duplicate.db
      .selectFrom('uploads')
      .selectAll()
      .where('id', '=', duplicate.id)
      .executeTakeFirstOrThrow()
    await duplicate.db
      .insertInto('uploads')
      .values({
        ...legacy,
        id: legacy.id + 100_000_000_000,
        folderName: `${legacy.folderName}-duplicate`,
      })
      .execute()
    const before = await duplicate.db
      .selectFrom('uploads')
      .selectAll()
      .where('key', '=', duplicate.identity.key)
      .execute()
    expect(
      await creationOk(await rpc('CreateCacheEntry', duplicate.token, duplicate.identity)),
    ).toBe(false)
    expect(
      await duplicate.db
        .selectFrom('uploads')
        .selectAll()
        .where('key', '=', duplicate.identity.key)
        .execute(),
    ).toEqual(before)
  })
})

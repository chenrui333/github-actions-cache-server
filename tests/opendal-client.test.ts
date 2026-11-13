import type { AddressInfo } from 'node:net'
import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import http from 'node:http'
import path from 'node:path'
import { execa } from 'execa'
import { SignJWT } from 'jose'
import { expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'

/** A real pinned OpenDAL client, not an emulated sequence of upload requests. */
test(
  'openDAL 0.55 whole/multipart cold writes and warm reads use authenticated exact completion',
  { timeout: 600_000 },
  async () => {
    const manifest = path.join(import.meta.dirname, 'clients/opendal/Cargo.toml')
    const cargo = process.env.CARGO ?? 'cargo'
    await execa(cargo, ['build', '--locked', '--manifest-path', manifest])
    const lengths: number[] = []
    let whole = 0
    let manifests = 0
    const failures: string[] = []
    const requests: { method: string | undefined; path: string; status?: number }[] = []
    const proxy = http.createServer((incoming, outgoing) => {
      const target = new URL(incoming.url!, 'http://127.0.0.1:3000')
      const record = {
        method: incoming.method,
        path: target.pathname,
        status: undefined as number | undefined,
      }
      requests.push(record)
      if (!['localhost', '127.0.0.1'].includes(target.hostname) || target.port !== '3000') {
        failures.push('Client attempted a non-fixture target')
        outgoing.writeHead(502).end()
        return
      }
      if (incoming.method === 'PUT' && /\/upload\/\d+$/.test(target.pathname)) {
        const operation = target.searchParams.get('comp')
        if (operation === 'block')
          lengths.push(Buffer.from(target.searchParams.get('blockid')!, 'base64').byteLength)
        else if (operation === 'blocklist') manifests++
        else whole++
      }
      const upstream = http.request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: target.host } },
        (response) => {
          record.status = response.statusCode
          outgoing.writeHead(response.statusCode!, response.headers)
          response.pipe(outgoing)
        },
      )
      upstream.on('error', () => {
        failures.push('Fixture proxy upstream failed')
        outgoing.writeHead(502).end()
      })
      incoming.pipe(upstream)
    })
    const listening = Promise.withResolvers<void>()
    proxy.once('error', listening.reject)
    proxy.listen(0, '127.0.0.1', listening.resolve)
    await listening.promise
    try {
      const proxyUrl = `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`
      const token = await new SignJWT({
        repository_id: 'opendal-client-proof',
        ac: JSON.stringify([{ Scope: 'refs/heads/main', Permission: 3 }]),
      })
        .setProtectedHeader({ alg: 'HS256' })
        .sign(Buffer.from('fixture-only-key'))
      const executable = path.join(
        import.meta.dirname,
        'clients/opendal/target/debug',
        process.platform === 'win32'
          ? 'cache-opendal-client-fixture.exe'
          : 'cache-opendal-client-fixture',
      )
      const result = await execa(executable, {
        env: {
          FIXTURE_ENDPOINT: 'http://127.0.0.1:3000/',
          FIXTURE_TOKEN: token,
          FIXTURE_PREFIX: randomUUID(),
          ACTIONS_CACHE_SERVICE_V2: 'true',
          GITHUB_SERVER_URL: 'https://github.com',
          HTTP_PROXY: proxyUrl,
          HTTPS_PROXY: proxyUrl,
          ALL_PROXY: proxyUrl,
          http_proxy: proxyUrl,
          https_proxy: proxyUrl,
          all_proxy: proxyUrl,
          NO_PROXY: '',
          no_proxy: '',
        },
      }).catch((err) => {
        console.error('OpenDAL fixture transport (no queries or credentials)', requests)
        throw err
      })
      expect(JSON.parse(result.stdout)).toEqual({
        whole: true,
        multipart: true,
        coldWarmExact: true,
      })
      expect(failures).toEqual([])
      expect(
        requests.every(({ status }) => status !== undefined && status >= 200 && status < 300),
      ).toBe(true)
      expect(whole).toBe(1)
      expect(manifests).toBe(1)
      expect(lengths.length).toBeGreaterThanOrEqual(2)
      expect(lengths.every((length) => length === 16)).toBe(true)
      const db = await getDatabase()
      const published = await db
        .selectFrom('cache_entries')
        .innerJoin('upload_receipts', 'upload_receipts.locationId', 'cache_entries.locationId')
        .innerJoin('storage_locations', 'storage_locations.id', 'cache_entries.locationId')
        .select(['kind', 'partNamesJson', 'upload_receipts.sizeBytes'])
        .where('repoId', '=', 'opendal-client-proof')
        .execute()
      expect(published).toHaveLength(2)
      expect(published.find(({ kind }) => kind === 'whole')?.sizeBytes).toBe(1024)
      const blocks = published.find(({ kind }) => kind === 'blocks')!
      expect(blocks.sizeBytes).toBe(3 * 64 * 1024)
      expect(JSON.parse(blocks.partNamesJson!).length).toBeGreaterThanOrEqual(2)
    } finally {
      const closed = Promise.withResolvers<void>()
      proxy.close((error) => (error ? closed.reject(error) : closed.resolve()))
      await closed.promise
    }
  },
)

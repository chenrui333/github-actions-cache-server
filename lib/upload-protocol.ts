import type { IncomingMessage } from 'node:http'
import type { Readable as ReadableType } from 'node:stream'
import { Buffer } from 'node:buffer'
import { createHash } from 'node:crypto'
import { Readable } from 'node:stream'
import { StringDecoder } from 'node:string_decoder'
import { SaxesParser } from 'saxes'

export interface UploadIdentity {
  key: string
  version: string
  scope: string
  repoId: string
}

export interface BlockReference {
  id: string
  source: 'Latest' | 'Uncommitted' | 'Committed'
}

export class UploadProtocolError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
  ) {
    super(message)
    this.name = 'UploadProtocolError'
  }
}

export function validateBlockId(id: string) {
  if (id.length < 4 || id.length > 88)
    throw new UploadProtocolError(400, 'Block IDs must encode 1 to 64 opaque bytes')
  const bytes = Buffer.from(id, 'base64')
  if (bytes.length === 0 || bytes.length > 64 || bytes.toString('base64') !== id)
    throw new UploadProtocolError(
      400,
      'Block IDs must be canonical Base64 encoding 1 to 64 opaque bytes',
    )
  return bytes.length
}

/** EOF of an aborted/truncated HTTP body is never an authenticated completion. */
export function requestBody(request: IncomingMessage) {
  const length = request.headers['content-length']
  if (length !== undefined && !/^\d+$/.test(length))
    throw new UploadProtocolError(400, 'Invalid Content-Length')
  const expected = length === undefined ? undefined : Number(length)
  if (expected !== undefined && !Number.isSafeInteger(expected))
    throw new UploadProtocolError(400, 'Invalid Content-Length')
  const md5 = request.headers['content-md5']
  if (
    md5 !== undefined &&
    (Array.isArray(md5) ||
      Buffer.from(md5, 'base64').length !== 16 ||
      Buffer.from(md5, 'base64').toString('base64') !== md5)
  )
    throw new UploadProtocolError(400, 'Invalid Content-MD5')
  const checksum = md5 === undefined ? undefined : createHash('md5')
  return Readable.from(
    (async function* () {
      let size = 0
      for await (const chunk of request) {
        size += chunk.length
        checksum?.update(chunk)
        yield chunk
      }
      if (!request.complete || (expected !== undefined && size !== expected))
        throw new UploadProtocolError(400, 'Incomplete upload request body')
      if (checksum && checksum.digest('base64') !== md5)
        throw new UploadProtocolError(400, 'Content-MD5 does not match')
    })(),
  )
}

function fail() {
  throw new UploadProtocolError(400, 'Invalid Azure block-list manifest')
}
/** Azure's ordered block manifest, not counters or a numeric block-ID suffix. */
export async function parseBlockList(body: ReadableType) {
  const blocks: BlockReference[] = []
  const sources = new Map<string, BlockReference['source']>()
  const decoder = new StringDecoder('utf8')
  const parser = new SaxesParser()
  let depth = 0
  let root = false
  let source: BlockReference['source'] | undefined
  let value = ''
  let idLength: number | undefined
  let bytes = 0
  parser.on('doctype', fail)
  parser.on('error', fail)
  parser.on('opentag', (tag) => {
    if (Object.keys(tag.attributes).length > 0) fail()
    if (depth === 0) {
      if (root || tag.name !== 'BlockList') fail()
      root = true
    } else if (depth === 1 && ['Latest', 'Uncommitted', 'Committed'].includes(tag.name)) {
      source = tag.name as BlockReference['source']
      value = ''
    } else fail()
    depth++
  })
  const text = (chunk: string) => {
    if (depth === 2) {
      value += chunk
      if (value.length > 128) fail()
    } else if (chunk.trim()) fail()
  }
  parser.on('text', text)
  parser.on('cdata', text)
  parser.on('closetag', () => {
    if (depth === 2) {
      const id = value.trim()
      const length = validateBlockId(id)
      if (idLength !== undefined && idLength !== length) fail()
      idLength = length
      if (!source || (sources.has(id) && sources.get(id) !== source)) fail()
      sources.set(id, source!)
      blocks.push({ id, source: source! })
      // Azure allows repeated IDs in one source list; each occurrence repeats
      // those exact bytes at that position. It caps the committed list at 50k.
      if (blocks.length > 50_000) fail()
      source = undefined
    }
    depth--
  })
  for await (const chunk of body) {
    bytes += chunk.length
    // 50k maximum IDs of 88 Base64 bytes plus bounded XML framing/whitespace.
    if (bytes > 50_000 * 128 + 1024)
      throw new UploadProtocolError(413, 'Azure block-list manifest is too large')
    parser.write(decoder.write(chunk))
  }
  parser.write(decoder.end()).close()
  if (!root || depth !== 0) fail()
  return blocks
}

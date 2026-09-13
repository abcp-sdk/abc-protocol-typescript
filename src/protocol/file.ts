/**
 * abc-protocol file access over NATS.
 *
 * Files are stored using the same shape the Go FileStore uses, so any host of
 * a `Bus` (the agent, extensions) can read/write shared file bytes + metadata:
 *   - bytes -> persistent (no-TTL) object store, keyed by `t.<tenant>.<code>`;
 *   - metadata -> KV bucket `abc-files-meta`, entries prefixed
 *     `t.<tenant>.f.` (record) and `t.<tenant>.sha.` (sha256 -> code dedup
 *     index).
 *
 * The tenant prefix is what keeps the dedup index from one tenant ever
 * resolving to another tenant's bytes.
 */
import type { Bus } from '../bus/index.js'
import { tenantKVKey, tenantObjectName } from './tenant.js'

export const FILE_META_BUCKET = 'abc-files-meta'
export const FILE_META_PREFIX = 'f.'
export const FILE_SHA_PREFIX = 'sha.'

export interface FileMeta {
  code: string
  sha256: string
  name: string
  mime: string
  size: number
  uploaderSession?: string
  createdAt: string
}

export interface FileRecord {
  meta: FileMeta
  data: Uint8Array
}

const NO_TTL = 0

export function fileKey(tenant: string, code: string): string {
  return tenantKVKey(tenant, FILE_META_PREFIX + code)
}

export function fileShaKey(tenant: string, sha256: string): string {
  return tenantKVKey(tenant, FILE_SHA_PREFIX + sha256)
}

export interface FileStore {
  put(tenant: string, code: string, meta: FileMeta, data: Uint8Array): Promise<void>
  get(tenant: string, code: string): Promise<FileRecord>
  stat(tenant: string, code: string): Promise<FileMeta | null>
  delete(tenant: string, code: string): Promise<void>
  /** Return the code of a previously stored file with `sha256` (dedup, per tenant). */
  bySha(tenant: string, sha256: string): Promise<string | null>
}

export function newFileStore(bus: Bus): FileStore {
  return new NatsFileStore(bus)
}

type FileMetaJSON = Omit<FileMeta, 'uploaderSession'> & {
  uploader_session?: string
}

class NatsFileStore implements FileStore {
  constructor(private readonly bus: Bus) {}

  async put(
    tenant: string,
    code: string,
    meta: FileMeta,
    data: Uint8Array,
  ): Promise<void> {
    if (code === '') throw new Error('file code required')
    // Bytes first (durable object bucket), then metadata. A crash between the
    // two only loses the dedup index (harmless re-upload).
    await this.bus.objectPutPersistent(tenantObjectName(tenant, code), data)
    const j = encodeMeta(meta)
    await this.bus.kvPut(
      FILE_META_BUCKET,
      fileKey(tenant, code),
      JSON.stringify(j),
      NO_TTL,
    )
    if (meta.sha256 !== '') {
      const created = await this.bus.kvCreate(
        FILE_META_BUCKET,
        fileShaKey(tenant, meta.sha256),
        code,
        NO_TTL,
      )
      if (created === null) {
        await this.bus.kvPut(
          FILE_META_BUCKET,
          fileShaKey(tenant, meta.sha256),
          code,
          NO_TTL,
        )
      }
    }
  }

  async get(tenant: string, code: string): Promise<FileRecord> {
    const data = await this.bus.objectGetPersistent(
      tenantObjectName(tenant, code),
    )
    if (data === null) throw new Error(`file not found: ${code}`)
    const meta = await this.stat(tenant, code)
    return { meta: meta ?? emptyMeta(code), data }
  }

  async stat(tenant: string, code: string): Promise<FileMeta | null> {
    const raw = await this.bus.kvGet(FILE_META_BUCKET, fileKey(tenant, code))
    if (raw === null || raw === '') return null
    return decodeMeta(raw) ?? null
  }

  async delete(tenant: string, code: string): Promise<void> {
    const meta = await this.stat(tenant, code)
    if (meta !== null && meta.sha256 !== '') {
      await this.bus.kvDelete(
        FILE_META_BUCKET,
        fileShaKey(tenant, meta.sha256),
      )
    }
    await this.bus.kvDelete(FILE_META_BUCKET, fileKey(tenant, code))
  }

  async bySha(tenant: string, sha256: string): Promise<string | null> {
    const code = await this.bus.kvGet(
      FILE_META_BUCKET,
      fileShaKey(tenant, sha256),
    )
    return code === null || code === '' ? null : code
  }
}

function emptyMeta(code: string): FileMeta {
  return {
    code,
    sha256: '',
    name: '',
    mime: '',
    size: 0,
    createdAt: '',
  }
}

function encodeMeta(m: FileMeta): FileMetaJSON {
  const out: FileMetaJSON = {
    code: m.code,
    sha256: m.sha256,
    name: m.name,
    mime: m.mime,
    size: m.size,
    createdAt: m.createdAt,
  }
  if (m.uploaderSession !== undefined) {
    out.uploader_session = m.uploaderSession
  }
  return out
}

function decodeMeta(raw: string): FileMeta | null {
  try {
    const v = JSON.parse(raw) as FileMetaJSON
    const code = String(v.code ?? '')
    if (code === '') return null
    return {
      code,
      sha256: String(v.sha256 ?? ''),
      name: String(v.name ?? ''),
      mime: String(v.mime ?? ''),
      size: Number(v.size ?? 0),
      uploaderSession: String(v.uploader_session ?? ''),
      createdAt: String(v.createdAt ?? ''),
    }
  } catch {
    return null
  }
}

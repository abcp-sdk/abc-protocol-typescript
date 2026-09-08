/**
 * abc-protocol file access over NATS.
 *
 * Files are stored using the same shape the Go FileStore uses, so any host of
 * a `Bus` (the agent, extensions) can read/write shared file bytes + metadata:
 *   - bytes -> persistent (no-TTL) object store, keyed by `code`;
 *   - metadata -> KV bucket `files.meta`, entries prefixed `f.` (record) and
 *     `sha.` (sha256 -> code dedup index).
 */
import type { Bus } from '../bus/index.js'

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

export function fileKey(code: string): string {
  return FILE_META_PREFIX + code
}

export function fileShaKey(sha256: string): string {
  return FILE_SHA_PREFIX + sha256
}

export interface FileStore {
  put(code: string, meta: FileMeta, data: Uint8Array): Promise<void>
  get(code: string): Promise<FileRecord>
  stat(code: string): Promise<FileMeta | null>
  delete(code: string): Promise<void>
  /** Return the code of a previously stored file with `sha256` (dedup). */
  bySha(sha256: string): Promise<string | null>
}

export function newFileStore(bus: Bus): FileStore {
  return new NatsFileStore(bus)
}

type FileMetaJSON = Omit<FileMeta, 'uploaderSession'> & {
  uploader_session?: string
}

class NatsFileStore implements FileStore {
  constructor(private readonly bus: Bus) {}

  async put(code: string, meta: FileMeta, data: Uint8Array): Promise<void> {
    if (code === '') throw new Error('file code required')
    // Bytes first (durable object bucket), then metadata. A crash between the
    // two only loses the dedup index (harmless re-upload).
    await this.bus.objectPutPersistent(code, data)
    const j = encodeMeta(meta)
    await this.bus.kvPut(FILE_META_BUCKET, fileKey(code), JSON.stringify(j), NO_TTL)
    if (meta.sha256 !== '') {
      const created = await this.bus.kvCreate(
        FILE_META_BUCKET,
        fileShaKey(meta.sha256),
        code,
        NO_TTL,
      )
      if (created === null) {
        await this.bus.kvPut(
          FILE_META_BUCKET,
          fileShaKey(meta.sha256),
          code,
          NO_TTL,
        )
      }
    }
  }

  async get(code: string): Promise<FileRecord> {
    const data = await this.bus.objectGetPersistent(code)
    if (data === null) throw new Error(`file not found: ${code}`)
    const meta = await this.stat(code)
    return { meta: meta ?? emptyMeta(code), data }
  }

  async stat(code: string): Promise<FileMeta | null> {
    const raw = await this.bus.kvGet(FILE_META_BUCKET, fileKey(code))
    if (raw === null || raw === '') return null
    return decodeMeta(raw) ?? null
  }

  async delete(code: string): Promise<void> {
    const meta = await this.stat(code)
    if (meta !== null && meta.sha256 !== '') {
      await this.bus.kvDelete(FILE_META_BUCKET, fileShaKey(meta.sha256))
    }
    await this.bus.kvDelete(FILE_META_BUCKET, fileKey(code))
  }

  async bySha(sha256: string): Promise<string | null> {
    const code = await this.bus.kvGet(FILE_META_BUCKET, fileShaKey(sha256))
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
  return {
    code: m.code,
    sha256: m.sha256,
    name: m.name,
    mime: m.mime,
    size: m.size,
    uploader_session: m.uploaderSession,
    createdAt: m.createdAt,
  }
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

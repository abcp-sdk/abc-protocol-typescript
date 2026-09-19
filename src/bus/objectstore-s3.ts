import type { ObjectStore } from './index.js'

/**
 * S3-compatible `ObjectStore` (AWS S3, MinIO, Ceph, …) — the TS twin of the Go
 * SDK's `bus.S3ObjectStore`. A deployment injects it into the bus so durable
 * file bytes (and, when chosen, transient objects) never sit in NATS.
 *
 * The AWS SDK is imported LAZILY and declared as an OPTIONAL peer dependency,
 * so extensions that never touch S3 neither install nor bundle it. Install
 * `@aws-sdk/client-s3` to use this class.
 */
export interface S3ObjectStoreOptions {
  bucket: string
  region?: string
  /** Custom endpoint (MinIO/OSS/…); omit for AWS S3. */
  endpoint?: string
  accessKeyId?: string
  secretAccessKey?: string
  forcePathStyle?: boolean
  /** Key prefix. Empty -> objects live at the bucket root. */
  prefix?: string
}

/** A structural view of `@aws-sdk/client-s3` limited to what we use, so this
 *  module needs no hard dependency on (or types for) the AWS SDK. */
interface S3ClientLike {
  send(cmd: unknown): Promise<{
    Body?: { transformToByteArray(): Promise<Uint8Array> }
  }>
}
interface S3Module {
  S3Client: new (cfg: {
    region: string
    endpoint?: string
    forcePathStyle?: boolean
    credentials?: { accessKeyId: string; secretAccessKey: string }
  }) => S3ClientLike
  GetObjectCommand: new (input: {
    Bucket: string
    Key: string
  }) => unknown
  PutObjectCommand: new (input: {
    Bucket: string
    Key: string
    Body: Uint8Array
  }) => unknown
}

/** Import the optional AWS SDK without a static type dependency on it. */
async function loadS3(): Promise<S3Module> {
  const spec = '@aws-sdk/client-s3'
  // Indirection keeps TypeScript from resolving (and requiring) the optional
  // module at build time.
  const mod = (await import(spec)) as unknown as S3Module
  return mod
}

export class S3ObjectStore implements ObjectStore {
  private readonly options: S3ObjectStoreOptions
  private clientPromise: Promise<S3ClientLike> | null = null
  private modPromise: Promise<S3Module> | null = null

  constructor(options: S3ObjectStoreOptions) {
    if (options.bucket === '') throw new Error('s3: bucket is required')
    this.options = options
  }

  private load(): Promise<S3Module> {
    this.modPromise ??= loadS3()
    return this.modPromise
  }

  private client(): Promise<S3ClientLike> {
    this.clientPromise ??= (async () => {
      const { S3Client } = await this.load()
      const o = this.options
      const haveCreds =
        o.accessKeyId !== undefined &&
        o.accessKeyId !== '' &&
        o.secretAccessKey !== undefined
      return new S3Client({
        region: o.region ?? 'us-east-1',
        ...(o.endpoint !== undefined
          ? { endpoint: o.endpoint, forcePathStyle: o.forcePathStyle ?? false }
          : {}),
        ...(haveCreds
          ? {
              credentials: {
                accessKeyId: o.accessKeyId as string,
                secretAccessKey: o.secretAccessKey as string,
              },
            }
          : {}),
      })
    })()
    return this.clientPromise
  }

  private key(name: string): string {
    const prefix = (this.options.prefix ?? '').replace(/\/+$/, '')
    return prefix === '' ? name : `${prefix}/${name}`
  }

  async objectPut(name: string, data: Uint8Array): Promise<void> {
    await this.objectPutPersistent(name, data)
  }

  async objectPutPersistent(name: string, data: Uint8Array): Promise<void> {
    const [client, { PutObjectCommand }] = await Promise.all([
      this.client(),
      this.load(),
    ])
    await client.send(
      new PutObjectCommand({
        Bucket: this.options.bucket,
        Key: this.key(name),
        Body: data,
      }),
    )
  }

  async objectGet(name: string): Promise<Uint8Array | null> {
    return this.objectGetPersistent(name)
  }

  async objectGetPersistent(name: string): Promise<Uint8Array | null> {
    const [client, { GetObjectCommand }] = await Promise.all([
      this.client(),
      this.load(),
    ])
    try {
      const out = await client.send(
        new GetObjectCommand({
          Bucket: this.options.bucket,
          Key: this.key(name),
        }),
      )
      if (out.Body === undefined) return null
      return new Uint8Array(await out.Body.transformToByteArray())
    } catch (e) {
      const name_ = (e as { name?: string }).name
      const status = (e as { $metadata?: { httpStatusCode?: number } })
        .$metadata?.httpStatusCode
      if (name_ === 'NoSuchKey' || name_ === 'NotFound' || status === 404) {
        return null
      }
      throw e
    }
  }
}

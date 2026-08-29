import { spawn } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { jetstreamManager } from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'

/**
 * Spawn a local nats-server as a child process — the "embedded" deployment
 * form of the single NATS transport. The agent side starts it (memory or
 * file JetStream storage); extensions always connect as clients.
 *
 * Binary resolution: opts.binary → ABC_NATS_SERVER_BIN → "nats-server"
 * (PATH lookup).
 */

export type Storage = 'memory' | 'file'

export interface ServerConfig {
  /** nats-server path. Default ABC_NATS_SERVER_BIN or "nats-server". */
  binary?: string
  /** 'memory' (default, nothing survives stop) or 'file' (persists to dir). */
  storage?: Storage
  /** JetStream store dir (file mode). Empty = temporary dir. */
  storeDir?: string
  /** Client port; 0 picks a free random port. */
  port?: number
}

export interface Server {
  url: string
  port: number
  stop: () => Promise<void>
}

function resolveBinary(explicit?: string): string {
  const candidates = [
    explicit,
    process.env.ABC_NATS_SERVER_BIN,
    'nats-server',
    // Common install locations for convenience.
    process.env.HOME !== undefined
      ? join(process.env.HOME, 'go', 'bin', 'nats-server')
      : undefined,
    '/usr/local/bin/nats-server',
    '/usr/bin/nats-server',
  ].filter((c): c is string => c !== undefined && c !== '')
  for (const bin of candidates) {
    if (bin.includes('/')) {
      if (existsSync(bin)) return bin
      if (bin !== 'nats-server') continue
    } else {
      // PATH lookup
      const pathEnv = process.env.PATH ?? ''
      for (const dir of pathEnv.split(':')) {
        if (dir !== '' && existsSync(join(dir, bin))) return bin
      }
    }
  }
  throw new Error(
    'natsrun: nats-server binary not found (set ABC_NATS_SERVER_BIN or install nats-server)',
  )
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer()
    srv.listen(0, '127.0.0.1', () => {
      const port = (srv.address() as { port: number }).port
      srv.close(() => resolve(port))
    })
    srv.on('error', reject)
  })
}

function waitReady(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    const tick = (): void => {
      const sock = createServer()
      sock.once('error', () => retry())
      sock.listen(port, '127.0.0.1', () => {
        sock.close(() => resolve())
      })
    }
    const retry = (): void => {
      if (Date.now() > deadline) {
        reject(new Error(`natsrun: server on port ${port} not ready`))
        return
      }
      setTimeout(tick, 60)
    }
    tick()
  })
}

/** Start a child nats-server and wait until it accepts connections. */
export async function start(opts: ServerConfig = {}): Promise<Server> {
  const bin = resolveBinary(opts.binary)
  const port = opts.port ?? (await freePort())
  const _fileStore = opts.storage === 'file'
  const dir = opts.storeDir ?? mkdtempSync(join(tmpdir(), 'abc-nats-'))
  if (opts.storeDir !== undefined && !existsSync(dir)) {
    throw new Error(`natsrun: storeDir does not exist: ${dir}`)
  }

  const args = ['-a', '127.0.0.1', '-p', String(port), '-js', '-sd', dir]
  const child = spawn(bin, args, { stdio: 'ignore' })
  // Spawn failures (ENOENT etc.) surface asynchronously on 'error'.
  const spawnError = new Promise<never>((_, reject) => {
    child.once('error', err => reject(err))
  })
  const exited = new Promise<never>((_, reject) => {
    child.once('exit', code =>
      reject(new Error(`natsrun: server exited early (code ${code})`)),
    )
  })
  const url = `nats://127.0.0.1:${port}`
  try {
    await Promise.race([waitReady(port, 10_000), spawnError, exited])
  } catch (e) {
    child.kill('SIGKILL')
    throw e
  }
  child.removeAllListeners('error')
  child.removeAllListeners('exit')

  if (opts.storage !== 'file') {
    // Memory mode: pre-create the mailbox/events stream with memory storage
    // so the hot queue path never touches disk. (KV/object buckets are
    // created lazily with file storage inside the temp dir — still
    // ephemeral across stop.)
    try {
      const nc = await connect({ servers: url })
      const jsm = await jetstreamManager(nc)
      await jsm.streams.add({
        name: 'ABC_MAILBOX',
        subjects: ['abc.mailbox.>', 'abc.session.events.>'],
        max_age: 24 * 3600 * 1_000_000_000,
        storage: 'memory',
      })
      await nc.close()
    } catch {
      // Pre-creation is an optimization; lazy creation still works.
    }
  }

  let stopped = false
  return {
    url,
    port,
    stop: async (): Promise<void> => {
      if (stopped) return
      stopped = true
      if (child.pid !== undefined) {
        child.kill('SIGTERM')
        await new Promise<void>(resolve => {
          const t = setTimeout(() => {
            child.kill('SIGKILL')
            resolve()
          }, 3000)
          child.once('exit', () => {
            clearTimeout(t)
            resolve()
          })
        })
      }
      if (opts.storeDir === undefined && existsSync(dir)) {
        try {
          rmSync(dir, { recursive: true, force: true })
        } catch {
          // best-effort cleanup
        }
      }
    },
  }
}

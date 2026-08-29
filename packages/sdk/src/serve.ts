import http from 'node:http'
import type { Bus } from './bus/index.js'
import { Extension, type ExtensionConfig } from './extension/index.js'

/**
 * One-stop main() body of an extension: register on the bus, serve the HTTP
 * surface, start optional workers, block until SIGINT/SIGTERM — then shut
 * down in order (workers → HTTP → bus).
 */
export async function serve(opts: {
  bus: Bus
  config: ExtensionConfig
  handler?: (req: http.IncomingMessage, res: http.ServerResponse) => void
  onStart?: (ext: Extension, signal: AbortSignal) => void
  port?: number
}): Promise<void> {
  const ext = new Extension(opts.bus, opts.config)
  await ext.serve()

  // ABC_PORT is the protocol-neutral name; ZERGX_PORT kept as a deployment
  // compat fallback for existing charts.
  const port =
    opts.port ?? Number(process.env.ABC_PORT ?? process.env.ZERGX_PORT ?? 808)
  const ac = new AbortController()
  opts.onStart?.(ext, ac.signal)

  const handler =
    opts.handler ??
    ((req, res) => {
      if (req.url === '/api/v1/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ ok: true, name: opts.config.id }))
        return
      }
      if (req.url === '/api/v1/version') {
        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(
          JSON.stringify({ id: opts.config.id, version: opts.config.version }),
        )
        return
      }
      res.writeHead(404)
      res.end('{}')
    })

  const server = http.createServer(handler)
  await new Promise<void>(resolve => server.listen(port, resolve))

  const shutdown = () => {
    ac.abort()
    server.close(() => {
      void ext.close().finally(() => process.exit(0))
    })
    setTimeout(() => process.exit(0), 5000).unref()
  }
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  await new Promise<void>(() => {})
}

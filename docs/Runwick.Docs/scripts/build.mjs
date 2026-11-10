// bun run build: type-check, build the client bundle, then the SSR bundle.
// Set INERTIA_SSR=0 for a client-only build; `process.execPath` is Bun here.
import { rmSync } from 'node:fs'
import { spawnSync } from 'node:child_process'

const run = (...args) => {
  const { status } = spawnSync(process.execPath, args, { stdio: 'inherit' })
  if (status) process.exit(status)
}

run('node_modules/typescript/bin/tsc', '--noEmit')
run('node_modules/vite/bin/vite.js', 'build')

if (process.env.INERTIA_SSR === '0') {
  rmSync('ssr', { recursive: true, force: true })
} else {
  run('node_modules/vite/bin/vite.js', 'build', '--ssr')
}

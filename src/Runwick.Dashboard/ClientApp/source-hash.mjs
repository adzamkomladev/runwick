// Writes ../dist/source.sha256 (21 §7.3): SHA-256 over every ClientApp file except node_modules/ and e2e/, in ordinal path
// order, each as `path \0 bytes \0`. A .NET test recomputes it, so a source change without a rebuild fails the build.
import { createHash } from 'node:crypto'
import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const root = import.meta.dirname
const files = []
const walk = (dir, prefix) => {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = prefix + entry.name
    if (entry.isDirectory()) {
      if (path !== 'node_modules' && path !== 'e2e') walk(join(dir, entry.name), path + '/')
    } else files.push(path)
  }
}
walk(root, '')
files.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)) // UTF-16 code-unit order, the same as .NET ordinal
const hash = createHash('sha256')
for (const path of files) {
  hash.update(Buffer.from(path, 'utf8')).update(Buffer.from([0])).update(readFileSync(join(root, path))).update(Buffer.from([0]))
}
writeFileSync(join(root, '../dist/source.sha256'), hash.digest('hex') + '\n')

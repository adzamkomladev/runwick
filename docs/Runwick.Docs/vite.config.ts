import fs from 'node:fs'
import path from 'node:path'
import inertia from '@inertiajs/vite'
import mdx from '@mdx-js/rollup'
import rehypeShiki from '@shikijs/rehype'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import rehypeSlug from 'rehype-slug'
import remarkFrontmatter from 'remark-frontmatter'
import remarkGfm from 'remark-gfm'
import remarkMdxFrontmatter from 'remark-mdx-frontmatter'
import { defineConfig, type Plugin } from 'vite'
import { runwickDark, runwickLight } from './code-themes'

// Inertia.Net reads the dev server origin from wwwroot/hot while the server runs.
function hotFile(): Plugin {
  const file = path.resolve(import.meta.dirname, 'wwwroot/hot')
  const remove = () => fs.rmSync(file, { force: true })
  return {
    name: 'runwick-docs-hot-file',
    apply: 'serve',
    configureServer(server) {
      server.httpServer?.once('listening', () => {
        fs.mkdirSync(path.dirname(file), { recursive: true })
        fs.writeFileSync(file, server.resolvedUrls?.local[0] ?? 'http://localhost:5170')
      })
      server.httpServer?.once('close', remove)
      for (const signal of ['SIGINT', 'SIGTERM'] as const) {
        process.once(signal, () => (remove(), process.exit()))
      }
      process.once('exit', remove)
    },
  }
}

export default defineConfig(({ command, isSsrBuild }) => ({
  base: command === 'build' ? '/build/' : '/',
  publicDir: false,
  plugins: [
    mdx({
      remarkPlugins: [remarkGfm, remarkFrontmatter, [remarkMdxFrontmatter, { name: 'frontmatter' }]],
      rehypePlugins: [
        rehypeSlug,
        [
          rehypeShiki,
          {
            themes: { light: runwickLight, dark: runwickDark },
            defaultColor: false,
            addLanguageClass: true,
            fallbackLanguage: 'text',
          },
        ],
      ],
    }),
    react(),
    tailwindcss(),
    inertia({
      ssr: {
        entry: 'ClientApp/ssr.tsx',
        host: '127.0.0.1',
        port: Number(process.env.INERTIA_SSR_PORT ?? 13715),
      },
    }),
    hotFile(),
  ],
  server: { port: 5170, strictPort: true, origin: 'http://localhost:5170' },
  // Bundle the SSR dependencies so the published bundle runs with only Node (the container does not ship node_modules).
  ssr: isSsrBuild ? { noExternal: true } : undefined,
  build: isSsrBuild
    ? { outDir: 'ssr', emptyOutDir: true }
    : {
        outDir: 'wwwroot/build',
        emptyOutDir: true,
        manifest: true,
        // The docs bundle intentionally includes every MDX page so Inertia navigation is instant.
        chunkSizeWarningLimit: 1400,
        rolldownOptions: { input: 'ClientApp/app.tsx' },
      },
}))

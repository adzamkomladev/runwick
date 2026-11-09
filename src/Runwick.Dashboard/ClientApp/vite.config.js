import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Relative base: the mount path (and any PathBase/proxy prefix) is only known at run time.
// One entry script and one stylesheet, no lazy chunks (21 §7.2): an old tab never asks a new server for a chunk it lacks.
// Tailwind compiles to that one static stylesheet; nothing is injected at run time (the CSP allows no inline style element).
export default defineConfig({
  base: './',
  publicDir: false,
  plugins: [react(), tailwindcss()],
  resolve: { alias: { '@': import.meta.dirname } },
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    manifest: 'manifest.json',
    modulePreload: { polyfill: false }, // the polyfill is an inline script; the CSP forbids it
    rolldownOptions: { input: 'app.jsx', output: { codeSplitting: false } },
  },
})

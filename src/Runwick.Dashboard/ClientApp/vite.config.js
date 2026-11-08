import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Relative base: the mount path (and any PathBase/proxy prefix) is only known at run time.
// One entry script and one stylesheet, no lazy chunks (21 §7.2): an old tab never asks a new server for a chunk it lacks.
export default defineConfig({
  base: './',
  publicDir: false,
  plugins: [react()],
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    manifest: 'manifest.json',
    modulePreload: { polyfill: false }, // the polyfill is an inline script; the CSP forbids it
    rolldownOptions: { input: 'app.jsx', output: { codeSplitting: false } },
  },
})

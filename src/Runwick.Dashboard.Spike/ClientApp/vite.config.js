import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Relative base: the mount path (and any PathBase/proxy prefix) is only known at runtime, so chunks resolve next to the entry.
export default defineConfig({
  base: './',
  publicDir: false,
  plugins: [react()],
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    manifest: 'manifest.json',
    modulePreload: { polyfill: false }, // the polyfill is an inline script; CSP forbids it
    rolldownOptions: { input: 'app.jsx' },
  },
})

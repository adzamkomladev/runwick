import { createInertiaApp } from '@inertiajs/react'
import { createRoot } from 'react-dom/client'
import './app.css'

const pages = import.meta.glob('./pages/*.jsx', { eager: true })

createInertiaApp({
  resolve: (name) => pages[`./pages/${name}.jsx`],
  setup: ({ el, App, props }) => createRoot(el).render(<App {...props} />),
  progress: false, // the progress bar injects an inline <style>; the dashboard CSP allows none
})

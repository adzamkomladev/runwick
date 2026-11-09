import { createInertiaApp } from '@inertiajs/react'
import { createRoot } from 'react-dom/client'
import { Toaster } from './components/ui/toast'
import { TooltipProvider } from './components/ui/tooltip'
import { applyTheme } from './lib'
import './app.css'

const pages = import.meta.glob('./pages/*.jsx', { eager: true })

applyTheme() // before the first render, so a dark theme never flashes light

createInertiaApp({
  resolve: (name) => pages[`./pages/${name}.jsx`],
  setup: ({ el, App, props }) => createRoot(el).render(
    <TooltipProvider>
      <App {...props} />
      <Toaster />
    </TooltipProvider>,
  ),
  progress: false, // the progress bar injects an inline <style>; the dashboard CSP allows none
})

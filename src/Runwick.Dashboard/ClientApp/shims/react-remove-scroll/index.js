// Runwick shim (see package.json): Radix overlays lock page scroll through this. The body gets data-scroll-locked while any
// lock is held; app.css turns that into overflow: hidden. No <style> element is created, so the CSP holds.
import { createElement, forwardRef, useEffect } from 'react'

let locks = 0

export const RemoveScroll = forwardRef(function RemoveScroll(
  // Props of the real component that have no meaning here are dropped so they never reach the DOM.
  { as = 'div', enabled = true, children, allowPinchZoom, shards, removeScrollBar, inert, noIsolation, noRelative, forwardProps, gapMode, lockRef, sideCar, ...rest },
  ref,
) {
  useEffect(() => {
    if (!enabled) return undefined
    if (locks++ === 0) document.body.setAttribute('data-scroll-locked', '')
    return () => { if (--locks === 0) document.body.removeAttribute('data-scroll-locked') }
  }, [enabled])
  return createElement(as, { ...rest, ref }, children)
})

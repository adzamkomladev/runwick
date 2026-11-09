// Runwick shim (see package.json) of aria-hidden's hideOthers: while a modal is open, everything outside it is hidden from
// assistive technology. Returns the undo function.
export function hideOthers(originalTarget, parentNode = document.body) {
  const targets = (Array.isArray(originalTarget) ? originalTarget : [originalTarget]).filter(Boolean)
  const hidden = []
  const walk = (parent) => {
    for (const el of parent.children) {
      if (targets.includes(el) || el.tagName === 'SCRIPT') continue
      if (targets.some((t) => el.contains(t))) walk(el)
      else if (!el.hasAttribute('aria-hidden')) {
        el.setAttribute('aria-hidden', 'true')
        hidden.push(el)
      }
    }
  }
  if (parentNode) walk(parentNode)
  return () => hidden.forEach((el) => el.removeAttribute('aria-hidden'))
}

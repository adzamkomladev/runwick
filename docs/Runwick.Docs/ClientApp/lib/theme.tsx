import { useCallback, useEffect, useMemo, useState } from 'react'

export type Theme = 'light' | 'dark' | 'system'

const storageKey = 'runwick-docs-theme'

const systemTheme = (): 'light' | 'dark' =>
  typeof matchMedia === 'function' && matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'

const readTheme = (): Theme => {
  try {
    const value = localStorage.getItem(storageKey)
    return value === 'light' || value === 'dark' || value === 'system' ? value : 'system'
  } catch {
    return 'system'
  }
}

const applyTheme = (theme: Theme) => {
  const resolved = theme === 'system' ? systemTheme() : theme
  document.documentElement.classList.toggle('dark', resolved === 'dark')
  document.documentElement.style.colorScheme = resolved
  return resolved
}

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => readTheme())
  const [resolvedTheme, setResolvedTheme] = useState<'light' | 'dark'>(() => (theme === 'system' ? systemTheme() : theme))

  const setTheme = useCallback((next: Theme) => {
    setThemeState(next)
    try {
      localStorage.setItem(storageKey, next)
    } catch {
      // Storage can be unavailable (private mode, embedded webviews); the class still applies.
    }
    setResolvedTheme(applyTheme(next))
  }, [])

  useEffect(() => {
    setResolvedTheme(applyTheme(theme))
    if (theme !== 'system') return

    const media = matchMedia('(prefers-color-scheme: dark)')
    const onChange = () => setResolvedTheme(applyTheme('system'))
    media.addEventListener('change', onChange)
    return () => media.removeEventListener('change', onChange)
  }, [theme])

  return useMemo(() => ({ theme, resolvedTheme, setTheme }), [theme, resolvedTheme, setTheme])
}

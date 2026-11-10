import { Link, usePage } from '@inertiajs/react'
import { Menu, Search } from 'lucide-react'
import { useEffect, useState } from 'react'
import { cn } from '../lib/utils'
import { Brand } from './Brand'
import { GithubIcon } from './icons/GithubIcon'
import { SearchDialog } from './SearchDialog'
import { ThemeToggle } from './ThemeToggle'
import { Kbd } from './ui/kbd'

const links = [
  { label: 'Docs', href: '/docs/introduction', match: (path: string) => path.startsWith('/docs/') && path !== '/docs/ai-agents' },
  { label: 'AI agents', href: '/docs/ai-agents', match: (path: string) => path === '/docs/ai-agents' },
]

const iconButton =
  'inline-flex size-9 items-center justify-center rounded-lg text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground active:scale-95'

export function SiteHeader({ onOpenNav, progress = false }: { onOpenNav?: () => void; progress?: boolean }) {
  const [searchOpen, setSearchOpen] = useState(false)
  const [scrolled, setScrolled] = useState(false)
  const [shortcut, setShortcut] = useState<string>()
  const { url } = usePage()
  const path = url.split('?')[0].split('#')[0]

  useEffect(() => {
    setShortcut(/mac|iphone|ipad/i.test(navigator.userAgent) ? '⌘K' : 'Ctrl K')
    const onScroll = () => setScrolled(window.scrollY > 4)
    onScroll()
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  return (
    <>
      <header
        className={cn(
          'sticky top-0 z-40 border-b bg-[var(--header-bg)] backdrop-blur-xl backdrop-saturate-150 transition-[border-color,box-shadow] duration-200',
          scrolled ? 'border-border shadow-[0_1px_0_0_var(--border),0_8px_24px_-16px_oklch(0.21_0.03_329/0.25)]' : 'border-transparent',
        )}
      >
        <div className="mx-auto flex h-14 max-w-[1440px] items-center gap-3 px-4 sm:px-6 lg:px-8">
          {onOpenNav && (
            <button type="button" onClick={onOpenNav} className={cn(iconButton, '-ml-1.5 lg:hidden')} aria-label="Open navigation">
              <Menu className="size-5" />
            </button>
          )}
          <Brand />
          <nav className="ml-5 hidden items-center gap-0.5 text-sm sm:flex" aria-label="Primary">
            {links.map((link) => {
              const active = link.match(path)
              return (
                <Link
                  key={link.href}
                  href={link.href}
                  aria-current={active ? 'page' : undefined}
                  className={cn(
                    'relative rounded-md px-3 py-1.5 font-medium transition-colors duration-150',
                    active ? 'text-foreground' : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {link.label}
                  <span
                    aria-hidden
                    className={cn(
                      'absolute inset-x-3 -bottom-[13px] h-px bg-primary transition-opacity duration-200',
                      active ? 'opacity-100' : 'opacity-0',
                    )}
                  />
                </Link>
              )
            })}
            <a
              href="https://github.com/adzamkomladev/runwick/tree/main/samples"
              target="_blank"
              rel="noreferrer"
              className="rounded-md px-3 py-1.5 font-medium text-muted-foreground transition-colors duration-150 hover:text-foreground"
            >
              Samples
            </a>
          </nav>

          <div className="ml-auto flex items-center gap-1">
            <button
              type="button"
              onClick={() => setSearchOpen(true)}
              className="group mr-1 hidden h-9 w-56 items-center gap-2.5 rounded-lg border border-border bg-card/60 pl-3 pr-1.5 text-left text-sm text-muted-foreground shadow-[0_1px_2px_oklch(0.21_0.03_329/0.04)] transition-[border-color,background-color,color] duration-150 hover:border-border-strong hover:bg-card hover:text-foreground md:flex lg:w-64"
            >
              <Search className="size-4 transition-transform duration-200 group-hover:scale-110" />
              <span className="flex-1">Search docs</span>
              {/* The platform label appears after mount so it never swaps under the reader. */}
              <Kbd className={cn('h-6 min-w-11 px-1.5 text-[11px] transition-opacity duration-200', !shortcut && 'opacity-0')}>{shortcut ?? '⌘K'}</Kbd>
            </button>
            <button type="button" onClick={() => setSearchOpen(true)} className={cn(iconButton, 'md:hidden')} aria-label="Search documentation">
              <Search className="size-[18px]" />
            </button>
            <ThemeToggle />
            <a href="https://github.com/adzamkomladev/runwick" target="_blank" rel="noreferrer" aria-label="Runwick on GitHub" className={iconButton}>
              <GithubIcon className="size-[18px]" />
            </a>
          </div>
        </div>
        {progress && <div aria-hidden className="reading-progress absolute inset-x-0 -bottom-px h-0.5 bg-primary" />}
      </header>
      <SearchDialog open={searchOpen} onOpenChange={setSearchOpen} />
    </>
  )
}

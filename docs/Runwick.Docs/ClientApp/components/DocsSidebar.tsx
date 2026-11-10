import { Link, usePage } from '@inertiajs/react'
import { useEffect, useRef } from 'react'
import { navigation } from '../lib/content'
import { cn } from '../lib/utils'

export function SidebarNav({ onNavigate }: { onNavigate?: () => void }) {
  const { url } = usePage()
  const path = url.split('?')[0].split('#')[0]
  const ref = useRef<HTMLElement>(null)

  // Keep the current page visible when the sidebar is taller than the viewport.
  useEffect(() => {
    const active = ref.current?.querySelector<HTMLElement>('[aria-current="page"]')
    const scroller = active?.closest<HTMLElement>('[data-sidebar-scroll]')
    if (!active || !scroller) return
    const { top, bottom } = active.getBoundingClientRect()
    const bounds = scroller.getBoundingClientRect()
    if (top < bounds.top + 24 || bottom > bounds.bottom - 24) {
      scroller.scrollTop += top - bounds.top - bounds.height / 3
    }
  }, [path])

  return (
    <nav ref={ref} className="space-y-6 pb-10 text-sm" aria-label="Documentation">
      {navigation.map((section) => (
        <div key={section.title}>
          <p className="mb-1.5 px-2.5 text-xs font-semibold text-foreground">{section.title}</p>
          <ul className="space-y-px">
            {section.pages.map((page) => {
              const active = path === page.url
              return (
                <li key={page.slug}>
                  <Link
                    href={page.url}
                    onClick={onNavigate}
                    prefetch="hover"
                    aria-current={active ? 'page' : undefined}
                    className={cn(
                      'group relative flex items-center rounded-md px-2.5 py-[5px] leading-5 transition-[background-color,color] duration-150',
                      active
                        ? 'bg-primary-soft font-medium text-primary'
                        : 'text-muted-foreground hover:bg-accent hover:text-foreground',
                    )}
                  >
                    <span className="transition-transform duration-200 ease-[var(--ease-out)] group-hover:translate-x-0.5 group-aria-[current=page]:translate-x-0">
                      {page.title}
                    </span>
                  </Link>
                </li>
              )
            })}
          </ul>
        </div>
      ))}
    </nav>
  )
}

export function DocsSidebar() {
  return (
    <aside className="hidden w-60 shrink-0 lg:block">
      <div
        data-sidebar-scroll
        className="scrollbar-thin sticky top-14 -ml-2.5 h-[calc(100vh-3.5rem)] overflow-y-auto overscroll-contain px-2.5 pt-8 [mask-image:linear-gradient(to_bottom,transparent,black_1.5rem,black_calc(100%-3rem),transparent)]"
      >
        <SidebarNav />
      </div>
    </aside>
  )
}

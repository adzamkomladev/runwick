import { ArrowUp, Pencil } from 'lucide-react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { Heading } from '../lib/content'
import { cn } from '../lib/utils'

const useIsomorphicLayoutEffect = typeof window === 'undefined' ? useEffect : useLayoutEffect

export function useActiveHeading(headings: Heading[]) {
  const [active, setActive] = useState(headings[0]?.id)

  useEffect(() => {
    setActive(headings[0]?.id)
    const elements = headings.map((heading) => document.getElementById(heading.id)).filter((element): element is HTMLElement => Boolean(element))
    if (elements.length === 0) return

    // The active heading is the last one whose top has passed the reading line under the header.
    let frame = 0
    const update = () => {
      frame = 0
      const line = 120
      let current = elements[0].id
      for (const element of elements) {
        if (element.getBoundingClientRect().top - line <= 0) current = element.id
        else break
      }
      if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4) {
        current = elements[elements.length - 1].id
      }
      setActive(current)
    }
    const schedule = () => {
      if (!frame) frame = requestAnimationFrame(update)
    }

    update()
    window.addEventListener('scroll', schedule, { passive: true })
    window.addEventListener('resize', schedule)
    return () => {
      cancelAnimationFrame(frame)
      window.removeEventListener('scroll', schedule)
      window.removeEventListener('resize', schedule)
    }
  }, [headings])

  return active
}

export function TableOfContents({ headings, slug }: { headings: Heading[]; slug: string }) {
  const active = useActiveHeading(headings)
  const listRef = useRef<HTMLUListElement>(null)
  const [marker, setMarker] = useState<{ top: number; height: number } | null>(null)

  // The marker slides along the rail to the active entry.
  useIsomorphicLayoutEffect(() => {
    const link = listRef.current?.querySelector<HTMLElement>(`[data-id="${CSS.escape(active ?? '')}"]`)
    if (!link) return setMarker(null)
    setMarker({ top: link.offsetTop, height: link.offsetHeight })
    link.scrollIntoView({ block: 'nearest' })
  }, [active])

  if (headings.length === 0) return null

  return (
    <aside className="hidden w-56 shrink-0 xl:block">
      <div className="scrollbar-thin sticky top-14 flex max-h-[calc(100vh-3.5rem)] flex-col overflow-y-auto pb-8 pt-12">
        <p className="mb-3 text-xs font-semibold text-foreground">On this page</p>
        <ul ref={listRef} className="relative border-l border-border">
          <span
            aria-hidden
            className={cn(
              'absolute -left-px w-px bg-primary transition-[transform,height,opacity] duration-300 ease-[var(--ease-out)]',
              marker ? 'opacity-100' : 'opacity-0',
            )}
            style={{ height: marker?.height ?? 0, transform: `translateY(${marker?.top ?? 0}px)` }}
          />
          {headings.map((heading) => (
            <li key={heading.id}>
              <a
                href={`#${heading.id}`}
                data-id={heading.id}
                aria-current={active === heading.id ? 'location' : undefined}
                className={cn(
                  'block py-1 text-[13px] leading-5 transition-colors duration-150',
                  heading.depth === 2 ? 'pl-3.5' : 'pl-6',
                  active === heading.id ? 'font-medium text-primary' : 'text-muted-foreground hover:text-foreground',
                )}
              >
                {heading.text}
              </a>
            </li>
          ))}
        </ul>
        <div className="mt-6 space-y-2.5 border-t border-border pt-5 text-xs">
          <a
            href={`https://github.com/adzamkomladev/runwick/edit/main/docs/Runwick.Docs/content/docs/${slug}.mdx`}
            target="_blank"
            rel="noreferrer"
            className="group flex items-center gap-2 text-muted-foreground transition-colors hover:text-foreground"
          >
            <Pencil className="size-3.5 transition-transform duration-200 group-hover:-rotate-12" />
            Edit this page
          </a>
          <a href="#top" onClick={(event) => (event.preventDefault(), window.scrollTo({ top: 0 }), history.replaceState(null, '', location.pathname))} className="group flex items-center gap-2 text-muted-foreground transition-colors hover:text-foreground">
            <ArrowUp className="size-3.5 transition-transform duration-200 group-hover:-translate-y-0.5" />
            Back to top
          </a>
        </div>
      </div>
    </aside>
  )
}

export function InlineContents({ headings }: { headings: Heading[] }) {
  const active = useActiveHeading(headings)
  if (headings.length < 2) return null

  return (
    <details className="group/toc mb-10 rounded-xl border border-border bg-card xl:hidden">
      <summary className="flex cursor-pointer list-none items-center justify-between px-4 py-3 text-sm font-medium [&::-webkit-details-marker]:hidden">
        On this page
        <span aria-hidden className="text-muted-foreground transition-transform duration-200 group-open/toc:rotate-180">
          <svg viewBox="0 0 16 16" className="size-4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"><path d="m4 6 4 4 4-4" /></svg>
        </span>
      </summary>
      <ul className="border-t border-border px-2 py-2 text-sm">
        {headings.map((heading) => (
          <li key={heading.id}>
            <a
              href={`#${heading.id}`}
              className={cn(
                'block rounded-md py-1.5 pr-2 transition-colors hover:bg-accent hover:text-foreground',
                heading.depth === 2 ? 'pl-2' : 'pl-6',
                active === heading.id ? 'font-medium text-primary' : 'text-muted-foreground',
              )}
            >
              {heading.text}
            </a>
          </li>
        ))}
      </ul>
    </details>
  )
}

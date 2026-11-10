import { Children, isValidElement, useEffect, useRef, useState, type ReactElement, type ReactNode } from 'react'
import { cn } from '../lib/utils'

interface TabProps {
  label: string
  children: ReactNode
}

const storageKey = 'runwick-docs-tabs'
const changeEvent = 'runwick-docs-tab-change'

// Tab groups that share a label (PostgreSQL, SQLite, Redis...) follow the reader's last choice across pages.
function readPreferred(): string[] {
  try {
    const value = JSON.parse(localStorage.getItem(storageKey) ?? '[]')
    return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
  } catch {
    return []
  }
}

function remember(label: string) {
  const next = [label, ...readPreferred().filter((item) => item !== label)].slice(0, 8)
  try {
    localStorage.setItem(storageKey, JSON.stringify(next))
  } catch {
    // Storage can be unavailable; the choice still applies to this page.
  }
  window.dispatchEvent(new CustomEvent(changeEvent, { detail: label }))
}

export function Tabs({ children, defaultValue }: { children: ReactNode; defaultValue?: string }) {
  const tabs = Children.toArray(children).filter((child): child is ReactElement<TabProps> => isValidElement(child)) as ReactElement<TabProps>[]
  const labels = tabs.map((tab) => tab.props.label)
  const [active, setActive] = useState(defaultValue ?? labels[0])
  const current = tabs.find((tab) => tab.props.label === active) ?? tabs[0]
  const listRef = useRef<HTMLDivElement>(null)
  const [indicator, setIndicator] = useState<{ left: number; width: number } | null>(null)
  const labelKey = labels.join('|')

  useEffect(() => {
    const preferred = readPreferred().find((label) => labels.includes(label))
    if (preferred) setActive(preferred)
    const onChange = (event: Event) => {
      const label = (event as CustomEvent<string>).detail
      if (labels.includes(label)) setActive(label)
    }
    window.addEventListener(changeEvent, onChange)
    return () => window.removeEventListener(changeEvent, onChange)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [labelKey])

  useEffect(() => {
    const button = listRef.current?.querySelector<HTMLElement>('[aria-selected="true"]')
    if (button) setIndicator({ left: button.offsetLeft, width: button.offsetWidth })
  }, [active, labelKey])

  const select = (label: string) => {
    // Keep the clicked tab under the pointer when other groups on the page change height.
    const button = listRef.current
    const before = button?.getBoundingClientRect().top ?? 0
    setActive(label)
    remember(label)
    requestAnimationFrame(() => {
      const after = button?.getBoundingClientRect().top ?? 0
      if (Math.abs(after - before) > 1) window.scrollBy({ top: after - before, behavior: 'instant' as ScrollBehavior })
    })
  }

  const onKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    const index = labels.indexOf(current?.props.label ?? '')
    const offset = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0
    if (!offset) return
    event.preventDefault()
    const next = labels[(index + offset + labels.length) % labels.length]
    select(next)
    listRef.current?.querySelector<HTMLElement>(`[data-label="${CSS.escape(next)}"]`)?.focus()
  }

  return (
    <div className="my-7 overflow-hidden rounded-xl border border-border bg-code">
      <div
        ref={listRef}
        role="tablist"
        onKeyDown={onKeyDown}
        className="no-scrollbar not-prose relative flex items-center gap-1 overflow-x-auto border-b border-border bg-muted/50 px-2"
      >
        {tabs.map((tab) => {
          const selected = tab.props.label === current?.props.label
          return (
            <button
              key={tab.props.label}
              type="button"
              role="tab"
              data-label={tab.props.label}
              aria-selected={selected}
              tabIndex={selected ? 0 : -1}
              onClick={() => select(tab.props.label)}
              className={cn(
                'relative shrink-0 rounded-md px-2.5 py-2.5 text-xs font-medium transition-colors duration-150',
                selected ? 'text-foreground' : 'text-muted-foreground hover:text-foreground',
              )}
            >
              {tab.props.label}
            </button>
          )
        })}
        <span
          aria-hidden
          className={cn('absolute bottom-0 left-0 h-0.5 rounded-full bg-primary transition-[transform,width] duration-300 ease-[var(--ease-out)]', !indicator && 'opacity-0')}
          style={{ width: indicator?.width ?? 0, transform: `translateX(${indicator?.left ?? 0}px)` }}
        />
      </div>
      <div
        role="tabpanel"
        className="[&>figure]:my-0 [&>figure]:rounded-none [&>figure]:border-0 [&>figure]:shadow-none [&>figure:not(:first-child)]:border-t [&>figure:not(:first-child)]:border-border [&>p]:m-0 [&>p]:px-4 [&>p]:py-3 [&>p]:text-sm [&>p]:leading-6 [&>p]:text-muted-foreground"
      >
        {current?.props.children}
      </div>
    </div>
  )
}

export function Tab(_props: TabProps) {
  return null
}

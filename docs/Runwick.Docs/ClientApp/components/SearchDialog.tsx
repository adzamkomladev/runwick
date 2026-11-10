import * as Dialog from '@radix-ui/react-dialog'
import { router } from '@inertiajs/react'
import { Command } from 'cmdk'
import { ArrowRight, CornerDownLeft, FileText, Hash, Search, SearchX, X } from 'lucide-react'
import { Fragment, useEffect, useMemo, useState, type ReactNode } from 'react'
import { getPage, getSearchIndex, type Heading, type SearchEntry } from '../lib/content'
import { Kbd } from './ui/kbd'

function score(entry: SearchEntry, query: string) {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean)
  if (terms.length === 0) return 1

  const title = entry.title.toLowerCase()
  const description = entry.description.toLowerCase()
  const headings = entry.headings.map((heading) => heading.toLowerCase())
  const body = entry.body.toLowerCase()

  if (!terms.every((term) => [title, description, ...headings, body].some((value) => value.includes(term)))) {
    return 0
  }

  return terms.reduce((total, term) => {
    if (title === term) return total + 120
    if (title.startsWith(term)) return total + 80
    if (title.includes(term)) return total + 55
    if (headings.some((heading) => heading === term)) return total + 40
    if (headings.some((heading) => heading.includes(term))) return total + 28
    if (description.includes(term)) return total + 18
    if (body.includes(term)) return total + 6
    return total
  }, 0)
}

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

// Marks the query terms inside a result so the reader sees why it matched.
function Highlight({ text, query }: { text: string; query: string }) {
  const terms = query.trim().split(/\s+/).filter(Boolean).map(escapeRegExp)
  if (terms.length === 0) return <>{text}</>
  const parts = text.split(new RegExp(`(${terms.join('|')})`, 'gi'))
  return (
    <>
      {parts.map((part, index) =>
        index % 2 === 1 ? (
          <mark key={index} className="rounded-[3px] bg-primary/15 px-px text-inherit">
            {part}
          </mark>
        ) : (
          <Fragment key={index}>{part}</Fragment>
        ),
      )}
    </>
  )
}

function matchingHeadings(entry: SearchEntry, query: string): Heading[] {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean)
  if (terms.length === 0) return []
  const headings = getPage(entry.id)?.headings ?? []
  return headings.filter((heading) => terms.every((term) => heading.text.toLowerCase().includes(term))).slice(0, 3)
}

const itemClass =
  'group flex cursor-pointer items-center gap-3 rounded-lg px-2.5 py-2 text-sm outline-none transition-colors duration-100 data-[selected=true]:bg-accent data-[selected=true]:text-accent-foreground'

function ItemIcon({ children }: { children: ReactNode }) {
  return (
    <span className="grid size-8 shrink-0 place-items-center rounded-md border border-border bg-muted/60 text-muted-foreground transition-colors duration-150 group-data-[selected=true]:border-primary/25 group-data-[selected=true]:bg-primary-soft group-data-[selected=true]:text-primary">
      {children}
    </span>
  )
}

export function SearchDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const [entries, setEntries] = useState<SearchEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [query, setQuery] = useState('')

  useEffect(() => {
    if (!open) return
    setLoading(true)
    getSearchIndex()
      .then(setEntries)
      .finally(() => setLoading(false))
  }, [open])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === 'k' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        onOpenChange(!open)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [open, onOpenChange])

  const select = (url: string) => {
    onOpenChange(false)
    setQuery('')
    router.visit(url)
  }

  useEffect(() => {
    if (!open) setQuery('')
  }, [open])

  const results = useMemo(() => {
    return entries
      .map((entry) => ({ entry, score: score(entry, query) }))
      .filter((result) => result.score > 0)
      .sort((a, b) => b.score - a.score || a.entry.title.localeCompare(b.entry.title))
      .slice(0, 50)
      .map((result) => result.entry)
  }, [entries, query])

  // Start each query on its best match instead of a stale selection further down the list.
  const [selected, setSelected] = useState('')
  useEffect(() => {
    setSelected(results[0]?.id ?? '')
    document.querySelector('[cmdk-list]')?.scrollTo({ top: 0 })
  }, [results])

  const groups = useMemo(() => {
    const grouped = new Map<string, SearchEntry[]>()
    for (const entry of results) {
      const items = grouped.get(entry.section) ?? []
      items.push(entry)
      grouped.set(entry.section, items)
    }
    return [...grouped.entries()]
  }, [results])

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="overlay-motion fixed inset-0 z-50 bg-[oklch(0.15_0.02_326/0.45)] backdrop-blur-sm" />
        <Dialog.Content
          aria-describedby={undefined}
          className="dialog-motion fixed left-1/2 top-[10vh] z-50 w-[calc(100%-2rem)] max-w-2xl -translate-x-1/2 overflow-hidden rounded-2xl border border-border bg-popover text-popover-foreground shadow-float"
        >
          <Dialog.Title className="sr-only">Search documentation</Dialog.Title>
          <Command shouldFilter={false} loop value={selected} onValueChange={setSelected} className="flex max-h-[min(72vh,600px)] flex-col">
            <div className="flex items-center gap-3 border-b border-border px-4">
              <Search className="size-[18px] shrink-0 text-primary" />
              <Command.Input
                value={query}
                onValueChange={setQuery}
                autoFocus
                placeholder="Search pages and sections..."
                className="h-14 w-full bg-transparent text-[15px] outline-none placeholder:text-muted-foreground"
              />
              <button
                type="button"
                onClick={() => onOpenChange(false)}
                className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                aria-label="Close search"
              >
                <X className="size-4" />
              </button>
            </div>
            <Command.List className="scrollbar-thin min-h-0 flex-1 overflow-y-auto overscroll-contain p-2 [&_[cmdk-list-sizer]]:transition-[height]">
              {loading && entries.length === 0 && <Command.Loading className="px-3 py-10 text-center text-sm text-muted-foreground">Loading the index...</Command.Loading>}
              <Command.Empty className="flex flex-col items-center gap-2 px-3 py-12 text-center text-sm text-muted-foreground">
                <SearchX className="size-6 text-muted-foreground/70" />
                <span>
                  No results for <span className="font-medium text-foreground">“{query}”</span>
                </span>
                <span className="text-xs">Try a job option such as WithMaxExecutions or an error code such as scheduler_not_active.</span>
              </Command.Empty>
              {groups.map(([section, items]) => (
                <Command.Group
                  key={section}
                  heading={section}
                  className="mb-1.5 [&_[cmdk-group-heading]]:px-2.5 [&_[cmdk-group-heading]]:pb-1.5 [&_[cmdk-group-heading]]:pt-2 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:text-muted-foreground"
                >
                  {items.map((entry) => (
                    <Fragment key={entry.id}>
                      <Command.Item value={entry.id} onSelect={() => select(entry.url)} className={itemClass}>
                        <ItemIcon>
                          <FileText className="size-4" />
                        </ItemIcon>
                        <span className="min-w-0 flex-1">
                          <span className="block truncate font-medium">
                            <Highlight text={entry.title} query={query} />
                          </span>
                          {entry.description && (
                            <span className="block truncate text-xs text-muted-foreground">
                              <Highlight text={entry.description} query={query} />
                            </span>
                          )}
                        </span>
                        <CornerDownLeft className="size-3.5 shrink-0 -translate-x-1 text-muted-foreground opacity-0 transition-[opacity,transform] duration-150 group-data-[selected=true]:translate-x-0 group-data-[selected=true]:opacity-100" />
                      </Command.Item>
                      {matchingHeadings(entry, query).map((heading) => (
                        <Command.Item
                          key={heading.id}
                          value={`${entry.id}#${heading.id}`}
                          onSelect={() => select(`${entry.url}#${heading.id}`)}
                          className={`${itemClass} ml-6 py-1.5`}
                        >
                          <Hash className="size-3.5 shrink-0 text-muted-foreground group-data-[selected=true]:text-primary" />
                          <span className="min-w-0 flex-1 truncate">
                            <Highlight text={heading.text} query={query} />
                          </span>
                          <ArrowRight className="size-3.5 shrink-0 -translate-x-1 text-muted-foreground opacity-0 transition-[opacity,transform] duration-150 group-data-[selected=true]:translate-x-0 group-data-[selected=true]:opacity-100" />
                        </Command.Item>
                      ))}
                    </Fragment>
                  ))}
                </Command.Group>
              ))}
            </Command.List>
            <div className="flex items-center gap-5 border-t border-border bg-muted/40 px-4 py-2.5 text-[11px] text-muted-foreground">
              <span className="flex items-center gap-1.5">
                <Kbd>↑</Kbd>
                <Kbd>↓</Kbd>
                Navigate
              </span>
              <span className="flex items-center gap-1.5">
                <Kbd>↵</Kbd>
                Open
              </span>
              <span className="ml-auto flex items-center gap-1.5">
                <Kbd>Esc</Kbd>
                Close
              </span>
            </div>
          </Command>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

import { IconCheck, IconSelector } from '@tabler/icons-react'
import { useId, useRef, useState } from 'react'
import { Popover, PopoverContent, PopoverTrigger } from './popover'
import { cn } from './utils'

/**
 * A searchable single choice (WAI-ARIA combobox with a listbox popup). Typing filters `options`; arrows move, Enter picks,
 * Escape closes. With `free`, the typed text itself can be chosen (a filter that also takes names the list does not have).
 * `value` '' means none.
 */
export function Combobox({ value, onChange, options, placeholder = 'Any', free = false, className, id, label }) {
  const [open, setOpen] = useState(false)
  const [text, setText] = useState('')
  const [active, setActive] = useState(0)
  const list = useId()
  const input = useRef(null)
  const shown = options.filter((o) => o.toLowerCase().includes(text.toLowerCase())).slice(0, 50)
  const items = free && text && !options.includes(text) ? [...shown, text] : shown
  const pick = (v) => { onChange(v); setOpen(false); setText('') }
  const onKey = (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setActive((a) => Math.min(a + 1, items.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setActive((a) => Math.max(a - 1, 0)) }
    else if (e.key === 'Enter') { e.preventDefault(); if (items[active] != null) pick(items[active]) }
  }
  return (
    <Popover open={open} onOpenChange={(o) => { setOpen(o); setActive(0) }}>
      <PopoverTrigger asChild>
        <button type="button" id={id} role="combobox" aria-expanded={open} aria-controls={list} aria-label={label}
          className={cn('flex h-9 w-full items-center justify-between gap-2 rounded-md border border-input bg-card px-3 text-sm shadow-xs outline-none transition-[color,box-shadow] focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50', !value && 'text-muted-foreground', className)}>
          <span className="truncate">{value || placeholder}</span><IconSelector className="size-4 shrink-0 opacity-50" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-(--radix-popover-trigger-width) min-w-52 p-1" onOpenAutoFocus={(e) => { e.preventDefault(); input.current?.focus() }}>
        <input ref={input} value={text} onChange={(e) => { setText(e.target.value); setActive(0) }} onKeyDown={onKey} placeholder="Search…"
          role="combobox" aria-expanded="true" aria-controls={list} aria-activedescendant={items.length ? `${list}-${active}` : undefined} aria-autocomplete="list"
          className="mb-1 h-8 w-full rounded-md border-b bg-transparent px-2 text-sm outline-none placeholder:text-muted-foreground/70" />
        <ul id={list} role="listbox" className="max-h-60 overflow-y-auto">
          {value && <li role="option" aria-selected={false} onClick={() => pick('')} className="cursor-default rounded-sm px-2 py-1.5 text-sm text-muted-foreground press hover:bg-accent">Clear</li>}
          {items.map((o, i) => (
            <li key={o} id={`${list}-${i}`} role="option" aria-selected={o === value} onMouseEnter={() => setActive(i)} onClick={() => pick(o)}
              className={cn('flex cursor-default items-center justify-between gap-2 rounded-sm px-2 py-1.5 text-sm press', i === active && 'bg-accent text-accent-foreground')}>
              <span className="truncate">{i === shown.length ? <>Use “{o}”</> : o}</span>{o === value && <IconCheck className="size-4" />}
            </li>
          ))}
          {items.length === 0 && <li className="px-2 py-6 text-center text-sm text-muted-foreground">No match.</li>}
        </ul>
      </PopoverContent>
    </Popover>
  )
}

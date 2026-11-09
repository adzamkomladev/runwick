import { router, usePage } from '@inertiajs/react'
import { IconCalendarEvent, IconChevronDown } from '@tabler/icons-react'
import { format } from 'date-fns'
import { useState } from 'react'
import { Button } from './ui/button'
import { Calendar } from './ui/calendar'
import { Input } from './ui/input'
import { Popover, PopoverContent, PopoverTrigger } from './ui/popover'
import { cn } from './ui/utils'

// The time range of a page (21 §4.2), kept in the URL query: `range=15m` (a preset, relative to each read, so polls move it) or
// `from`/`to` (fixed, ISO 8601 UTC). The server has the same preset list.

export const PRESETS = [
  ['5m', 'Last 5 minutes'], ['15m', 'Last 15 minutes'], ['30m', 'Last 30 minutes'], ['1h', 'Last hour'], ['3h', 'Last 3 hours'],
  ['6h', 'Last 6 hours'], ['1d', 'Last 24 hours'], ['3d', 'Last 3 days'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days'],
]
const SPAN = { m: 60_000, h: 3_600_000, d: 86_400_000 }
/** Milliseconds a preset covers. */
export const presetMs = (key) => (key ? Number(key.slice(0, -1)) * SPAN[key.at(-1)] : null)

/** Visits the current page with some query values changed (null removes one); any cursor is dropped. */
export function useQueryNav() {
  const { url } = usePage()
  return (changes) => {
    const u = new URL(url, window.location.href)
    u.searchParams.delete('cursor')
    for (const [k, v] of Object.entries(changes)) { if (v == null || v === '') u.searchParams.delete(k); else u.searchParams.set(k, v) }
    router.get(u.pathname + u.search, {}, { preserveScroll: true })
  }
}

const label = ({ range, from, to }, fallback) => {
  if (range) return PRESETS.find(([k]) => k === range)?.[1] ?? range
  if (from || to) {
    const f = (v) => format(new Date(v), 'MMM d, HH:mm')
    return `${from ? f(from) : '…'} – ${to ? f(to) : 'now'}`
  }
  return fallback ? PRESETS.find(([k]) => k === fallback)?.[1] : 'Any time'
}

const TIME = /^([01]?\d|2[0-3]):([0-5]\d)$/
const withTime = (day, hhmm) => { const [, h, m] = TIME.exec(hhmm); const d = new Date(day); d.setHours(+h, +m, 0, 0); return d }

/** The range button and its popover: presets on the left, a two-date calendar with times on the right. */
export function DateRangePicker({ query, fallback, className }) {
  const go = useQueryNav()
  const [open, setOpen] = useState(false)
  const [days, setDays] = useState(() => (query.from ? { from: new Date(query.from), to: query.to ? new Date(query.to) : undefined } : undefined))
  const [times, setTimes] = useState(() => [query.from ? format(new Date(query.from), 'HH:mm') : '00:00', query.to ? format(new Date(query.to), 'HH:mm') : '23:59'])
  const active = query.range ?? (!query.from && !query.to ? fallback : null)
  const valid = days?.from && TIME.test(times[0]) && TIME.test(times[1])
  const choose = (range) => { setOpen(false); go({ range, from: null, to: null }) }
  const apply = () => {
    const from = withTime(days.from, times[0]), to = withTime(days.to ?? days.from, times[1])
    setOpen(false)
    go({ range: null, from: from.toISOString(), to: (to < from ? from : to).toISOString() })
  }
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" data-testid="range" className={cn('justify-between font-normal', className)}>
          <IconCalendarEvent className="text-muted-foreground" /><span className="truncate">{label(query, fallback)}</span><IconChevronDown className="opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-auto max-w-[calc(100vw-2rem)] p-0" data-testid="range-popover">
        <div className="flex flex-col sm:flex-row">
          <ul aria-label="Quick ranges" className="grid grid-cols-2 gap-0.5 border-b p-2 sm:w-44 sm:grid-cols-1 sm:border-r sm:border-b-0">
            {PRESETS.map(([key, text]) => (
              <li key={key}>
                <button type="button" aria-pressed={active === key} onClick={() => choose(key)} data-testid={`range-${key}`}
                  className={cn('w-full rounded-md px-2.5 py-1.5 text-left text-sm outline-none transition-colors hover:bg-accent focus-visible:ring-[3px] focus-visible:ring-ring/50',
                    active === key && 'bg-primary text-primary-foreground hover:bg-primary')}>{text}</button>
              </li>
            ))}
            {!fallback && <li className="col-span-2 sm:col-span-1"><button type="button" aria-pressed={!active && !query.from} onClick={() => choose(null)}
              className="w-full rounded-md px-2.5 py-1.5 text-left text-sm text-muted-foreground outline-none hover:bg-accent focus-visible:ring-[3px] focus-visible:ring-ring/50">Any time</button></li>}
          </ul>
          <div className="grid gap-3 p-3">
            <Calendar mode="range" selected={days} onSelect={setDays} numberOfMonths={1} disabled={{ after: new Date() }} defaultMonth={days?.from} />
            <div className="grid grid-cols-2 gap-2">
              {['From', 'To'].map((t, i) => (
                <label key={t} className="grid gap-1 text-xs text-muted-foreground">{t} time
                  <Input value={times[i]} inputMode="numeric" placeholder="HH:mm" aria-invalid={!TIME.test(times[i]) || undefined} className="h-8 tabular-nums aria-[invalid]:border-destructive"
                    onChange={(e) => setTimes((ts) => ts.map((x, j) => (j === i ? e.target.value : x)))} />
                </label>
              ))}
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-xs text-muted-foreground">Local time</span>
              <Button size="sm" disabled={!valid} onClick={apply} data-testid="range-apply">Apply</Button>
            </div>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  )
}

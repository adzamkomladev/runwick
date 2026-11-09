import { Link, router, usePage } from '@inertiajs/react'
import {
  IconActivity, IconAlertTriangle, IconArrowDown, IconArrowRight, IconArrowUp, IconArrowsSort, IconBolt, IconCalendarTime, IconCheck,
  IconCircleDashed, IconCopy, IconDeviceDesktop, IconFilter, IconGauge, IconHistory, IconLayoutDashboard, IconListDetails, IconLoader2,
  IconMoon, IconServer2, IconSettings, IconStack2, IconSun, IconTrash, IconX,
} from '@tabler/icons-react'
import { useEffect, useRef, useState, useSyncExternalStore } from 'react'
import {
  AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from './components/ui/alert-dialog'
import { Badge } from './components/ui/badge'
import { Button } from './components/ui/button'
import { Card } from './components/ui/card'
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuLabel, DropdownMenuRadioGroup, DropdownMenuRadioItem, DropdownMenuTrigger,
} from './components/ui/dropdown-menu'
import { Input } from './components/ui/input'
import { Label } from './components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './components/ui/select'
import { Sidebar, SidebarContent, SidebarFooter, SidebarGroup, SidebarHeader, SidebarMenuButton, SidebarProvider, SidebarTrigger } from './components/ui/sidebar'
import { Skeleton } from './components/ui/skeleton'
import { TableHead } from './components/ui/table'
import { dismissToast, toast } from './components/ui/toast'
import { Tooltip, TooltipContent, TooltipTrigger } from './components/ui/tooltip'
import { cn } from './components/ui/utils'

// The antiforgery token is in the HTML <meta>, never in page props, so it never enters history state.
const csrf = () => document.querySelector('meta[name="runwick-csrf"]')?.content ?? ''

// Controls and reveals in flight on this page: polls wait so a refreshed revision cannot change under a submitted command.
let acting = 0

const MAX_BACKOFF = 60_000

/** True while the tab is hidden (polls pause then). */
const useHidden = () => useSyncExternalStore((l) => {
  document.addEventListener('visibilitychange', l)
  return () => document.removeEventListener('visibilitychange', l)
}, () => document.hidden)

/**
 * Polls the page's list props (21 §4.1): a partial reload of `only`, first page only (no cursor in the URL), one request in
 * flight, paused while the tab is hidden (one poll on becoming visible), and the interval doubles to 60 s on failure.
 * Returns the interval in seconds, or 0 when this page does not poll.
 */
export function usePoll(only, seconds) {
  const { url } = usePage()
  const key = only.join(',')
  const active = !!seconds && !!key && !new URL(url, window.location.href).searchParams.has('cursor')
  useEffect(() => {
    if (!active) return undefined
    const base = Math.max(seconds, 5) * 1000
    let delay = base, timer = 0, inFlight = false, stopped = false
    const schedule = () => {
      clearTimeout(timer)
      if (!stopped && !document.hidden) timer = setTimeout(tick, delay)
    }
    const tick = () => {
      if (inFlight || document.hidden) return
      if (acting > 0) return schedule()
      inFlight = true
      let ok = false
      router.reload({
        only: key.split(','),
        async: true,
        onSuccess: () => { ok = true },
        onHttpException: () => false, // 429/5xx: no error dialog, just back off
        onNetworkError: () => false,
        onFinish: () => {
          inFlight = false
          delay = ok ? base : Math.min(delay * 2, MAX_BACKOFF)
          schedule()
        },
      })
    }
    const visibility = () => {
      clearTimeout(timer)
      if (!document.hidden) tick()
    }
    document.addEventListener('visibilitychange', visibility)
    schedule()
    return () => {
      stopped = true
      clearTimeout(timer)
      document.removeEventListener('visibilitychange', visibility)
    }
  }, [url, key, seconds, active])
  return active ? Math.max(seconds, 5) : 0
}

const RESULT_TOAST = 'command-result'

/**
 * One control at a time (21 §5): confirm, then post with a fresh command ID. "Try again" and "Check again" resend the same
 * command ID and body. Success is shown from the request's callback, not from a prop: "done" and rejections as a toast, the
 * answers that need a next step (unknown, failed, bulk) inline in CommandBar.
 */
export function useCommand() {
  const [state, setState] = useState(null)
  const send = (s) => {
    acting++
    dismissToast(RESULT_TOAST)
    setState({ ...s, status: 'sending' })
    router.post(s.url, s.body, {
      headers: { 'X-Runwick-Csrf': csrf(), 'X-Runwick-Command-Id': s.commandId },
      preserveScroll: true,
      onSuccess: (page) => {
        const outcome = page.props.outcome
        const status = !outcome ? 'done' : outcome.items ? 'bulk' : outcome.code === 'outcome_unknown' ? 'unknown'
          : ['store_unavailable', 'query_budget_exceeded', 'busy'].includes(outcome.code) ? 'failed' : 'rejected'
        if (status === 'done') {
          toast({ id: RESULT_TOAST, variant: 'success', testId: 'done', title: s.label, description: s.done })
        } else if (status === 'rejected') {
          toast({ id: RESULT_TOAST, variant: 'destructive', testId: 'rejected', title: `${s.label}: not applied`,
            description: <>Code <code>{outcome.code}</code>{outcome.actualRevision != null && <> (current revision {outcome.actualRevision})</>}. The page shows the current state.</> })
        }
        setState(status === 'done' || status === 'rejected' ? null : { ...s, status, outcome })
      },
      onHttpException: (response) => {
        setState({ ...s, status: 'failed', outcome: { code: response.data?.code ?? `http_${response.status}` } })
        return false
      },
      onNetworkError: () => {
        setState({ ...s, status: 'failed', outcome: { code: 'no_answer' } })
        return false
      },
      onFinish: () => { acting-- },
    })
  }
  return {
    state,
    ask: (label, url, body, done = 'done') => setState({ label, url, body, done, status: 'confirm' }),
    confirm: () => send({ ...state, commandId: crypto.randomUUID() }),
    again: () => send(state),
    close: () => setState(null),
    bulk: (label, url, items, reason) =>
      send({ label, url, body: { items: items.map((i) => ({ ...i, commandId: crypto.randomUUID() })), reason }, done: 'done', commandId: '' }),
    /** True while this URL's command is being sent: the button that started it shows a spinner. */
    busy: (url) => state?.status === 'sending' && state.url === url,
  }
}

/** A button that asks for a control; it shows a spinner while that control is being sent. */
export function CommandButton({ command, url, onClick, icon: Icon, children, ...props }) {
  const busy = command.busy(url)
  return (
    <Button size="sm" variant="outline" onClick={onClick} disabled={busy || props.disabled} aria-busy={busy || undefined} {...props}>
      {busy ? <IconLoader2 className="animate-spin" /> : Icon && <Icon />}
      {children}
    </Button>
  )
}

/** The confirm dialog, and the answers that need a next step: unknown (Check again), failed (Try again) and bulk results. */
export function CommandBar({ command }) {
  const s = command.state
  const o = s?.outcome
  return (
    <>
      <AlertDialog open={s?.status === 'confirm'} onOpenChange={(open) => { if (!open) command.close() }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{s?.label}</AlertDialogTitle>
            <AlertDialogDescription>Are you sure? The change is checked against the revision this page shows.</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Back</AlertDialogCancel>
            <Button data-testid="confirm" onClick={command.confirm}>Confirm</Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      {s && ['unknown', 'failed', 'bulk', 'sending'].includes(s.status) && (
        <div role="status" data-testid="command" data-status={s.status}
          className={cn('flex animate-page-in items-start gap-3 rounded-lg border p-4 text-sm',
            s.status === 'failed' ? 'border-destructive/40 bg-destructive/5' : s.status === 'unknown' ? 'border-warning/40 bg-warning/5' : 'bg-card')}>
          {s.status === 'sending' ? <IconLoader2 className="mt-0.5 size-4 shrink-0 animate-spin text-primary" />
            : s.status === 'failed' ? <IconAlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" />
              : <IconActivity className="mt-0.5 size-4 shrink-0 text-warning" />}
          <div className="grid min-w-0 flex-1 gap-2">
            <strong className="font-medium">{s.label}</strong>
            {s.status === 'sending' && <span className="text-muted-foreground">Sending…</span>}
            {s.status === 'unknown' && <span>The result is not known yet (command <code>{o.commandId}</code>). It may still be applied.</span>}
            {s.status === 'failed' && <span>No result: <code data-testid="failed-code">{o.code}</code>. Nothing is known to have changed.</span>}
            {s.status === 'bulk' && <ul data-testid="bulk" className="grid gap-1">
              {o.items.map((i) => (
                <li key={i.jobId} className="flex flex-wrap items-center gap-2">
                  <code className="text-muted-foreground">{i.jobId}</code>
                  <Badge variant={i.code ? 'destructive' : 'success'}>{i.status}</Badge>{i.code && <code>{i.code}</code>}
                </li>
              ))}
            </ul>}
            {(s.status === 'unknown' || s.status === 'failed') && <div>
              <Button size="sm" data-testid="again" onClick={command.again}>{s.status === 'unknown' ? 'Check again' : 'Try again'}</Button>
            </div>}
          </div>
          {s.status !== 'sending' && <Button size="icon-sm" variant="ghost" onClick={command.close} aria-label="Dismiss"><IconX /></Button>}
        </div>
      )}
    </>
  )
}

/** A reveal (21 §5.1): plain fetch, the value kept in component state only and cleared on navigation. */
export function useReveal() {
  const [value, setValue] = useState(null)
  const [loading, setLoading] = useState(null)
  useEffect(() => { // 'success' also covers the 303 that follows a command on the same page
    const off = [router.on('navigate', () => setValue(null)), router.on('success', () => setValue(null))]
    return () => off.forEach((f) => f())
  }, [])
  const reveal = async (key, url) => {
    acting++
    setLoading(key)
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'X-Runwick-Csrf': csrf() },
        body: '{}',
        credentials: 'same-origin',
        cache: 'no-store',
      })
      const body = await response.json().catch(() => ({}))
      setValue({ key, ok: response.ok, text: response.ok ? JSON.stringify(body, null, 2)
        : response.status === 410 ? 'detail expired' : `${body.code ?? 'error'} (${response.status})` })
    } catch {
      setValue({ key, ok: false, text: 'no answer' })
    } finally {
      acting--
      setLoading(null)
    }
  }
  return { value, reveal, loading, clear: () => setValue(null) }
}

/** A revealed value: formatted text (never HTML), with copy and hide. */
export function Revealed({ value, testId, onHide, title }) {
  return (
    <div className="grid animate-page-in gap-2 rounded-lg border bg-muted/40 p-3">
      <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
        <span className="flex-1">{title}{!value.ok && ' (not shown)'}</span>
        {value.ok && <CopyButton value={value.text} label={`Copy ${title.toLowerCase()}`} />}
        <Button size="icon-sm" variant="ghost" onClick={onHide} aria-label={`Hide ${title.toLowerCase()}`}><IconX /></Button>
      </div>
      <pre data-testid={testId} className={cn('max-h-96 overflow-auto font-mono text-xs break-all whitespace-pre-wrap', !value.ok && 'text-destructive')}>{value.text}</pre>
    </div>
  )
}

// ---- Theme: light, dark or the system setting; the choice is kept in this browser only. -------------------------------------

const THEME_KEY = 'runwick-dashboard-theme'
const themeListeners = new Set()
const readTheme = () => { try { return localStorage.getItem(THEME_KEY) ?? 'system' } catch { return 'system' } }
const darkQuery = window.matchMedia('(prefers-color-scheme: dark)')
export function applyTheme() {
  const theme = readTheme()
  document.documentElement.classList.toggle('dark', theme === 'dark' || (theme === 'system' && darkQuery.matches))
}
darkQuery.addEventListener('change', applyTheme)
function setTheme(theme) {
  try { localStorage.setItem(THEME_KEY, theme) } catch { /* private mode: the choice lasts until reload */ }
  applyTheme()
  themeListeners.forEach((l) => l())
}

function ThemeToggle() {
  const theme = useSyncExternalStore((l) => { themeListeners.add(l); return () => themeListeners.delete(l) }, readTheme)
  return (
    <DropdownMenu>
      <Tooltip>
        <TooltipTrigger asChild>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon-sm" aria-label="Theme">
              <IconSun className="scale-100 rotate-0 transition-transform duration-300 dark:scale-0 dark:-rotate-90" />
              <IconMoon className="absolute scale-0 rotate-90 transition-transform duration-300 dark:scale-100 dark:rotate-0" />
            </Button>
          </DropdownMenuTrigger>
        </TooltipTrigger>
        <TooltipContent>Theme</TooltipContent>
      </Tooltip>
      <DropdownMenuContent align="end">
        <DropdownMenuLabel>Theme</DropdownMenuLabel>
        <DropdownMenuRadioGroup value={theme} onValueChange={setTheme}>
          <DropdownMenuRadioItem value="light"><IconSun />Light</DropdownMenuRadioItem>
          <DropdownMenuRadioItem value="dark"><IconMoon />Dark</DropdownMenuRadioItem>
          <DropdownMenuRadioItem value="system"><IconDeviceDesktop />System</DropdownMenuRadioItem>
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// ---- Layout -----------------------------------------------------------------------------------------------------------------

const NAV = [
  ['Monitor', [
    ['overview', 'Overview', '/overview', IconLayoutDashboard, ['Overview']],
    ['jobs', 'Jobs', '', IconListDetails, ['Jobs', 'Job']],
    ['queues', 'Queues', '/queues', IconStack2, ['Queues']],
    ['workers', 'Workers', '/workers', IconServer2, ['Workers']],
    ['schedules', 'Schedules', '/schedules', IconCalendarTime, ['Schedules']],
    ['scaling', 'Scaling', '/scaling', IconGauge, ['Scaling']],
  ]],
  ['Govern', [
    ['audit', 'Audit', '/audit', IconHistory, ['Audit']],
    ['retention', 'Retention', '/retention', IconTrash, ['Retention']],
    ['settings', 'Settings', '/settings', IconSettings, ['Settings']],
  ]],
]
const TITLES = Object.fromEntries(NAV.flatMap(([, items]) => items.flatMap(([, label, , , components]) => components.map((c) => [c, label]))))

function Nav() {
  const { component, props: { base, can } } = usePage()
  return (
    <nav data-testid="nav" aria-label="Dashboard" className="grid gap-4">
      {NAV.map(([group, items]) => {
        const shown = items.filter(([page]) => can[page])
        return shown.length > 0 && (
          <SidebarGroup key={group} label={group}>
            {shown.map(([page, label, path, Icon, components]) => (
              <SidebarMenuButton key={page} asChild active={components.includes(component)} tooltip={label}>
                <Link href={base + path} data-testid={`nav-${page}`}><Icon aria-hidden /><span>{label}</span></Link>
              </SidebarMenuButton>
            ))}
          </SidebarGroup>
        )
      })}
    </nav>
  )
}

function Brand() {
  const { title } = usePage().props
  return (
    <div className="flex h-9 items-center gap-2.5 px-0.5">
      <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-gradient-to-br from-primary to-primary/70 text-primary-foreground shadow-sm ring-1 ring-primary/20">
        <IconBolt className="size-4.5" aria-hidden />
      </span>
      <span className="grid min-w-0 leading-tight transition-opacity duration-200 group-data-[sidebar=collapsed]/sidebar:sr-only">
        <span className="truncate font-semibold tracking-tight" data-testid="title">{title}</span>
        <span className="truncate text-xs text-muted-foreground">Jobs dashboard</span>
      </span>
    </div>
  )
}

function UserRow() {
  const { user } = usePage().props
  return (
    <div className="flex h-10 min-w-0 items-center gap-2.5 px-0.5">
      <span aria-hidden className="grid size-8 shrink-0 place-items-center rounded-full bg-accent text-xs font-semibold text-accent-foreground ring-1 ring-border">{initials(user)}</span>
      <span className="grid min-w-0 text-sm leading-tight group-data-[sidebar=collapsed]/sidebar:sr-only">
        <span className="text-xs text-muted-foreground">Signed in as</span>
        <span className="truncate font-medium" data-testid="user">{user}</span>
      </span>
    </div>
  )
}

/** "Live" with a pulsing dot while the page polls; "Paused" while the tab is hidden; nothing when the page does not poll. */
function LiveIndicator({ seconds }) {
  const hidden = useHidden()
  if (!seconds) return null
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span tabIndex={0} className="inline-flex items-center gap-2 rounded-full border bg-card px-2.5 py-1 text-xs font-medium text-muted-foreground outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50">
          <span className={cn('size-2 rounded-full', hidden ? 'bg-muted-foreground/50' : 'animate-live bg-success')} />
          {hidden ? 'Paused' : 'Live'}
        </span>
      </TooltipTrigger>
      <TooltipContent>{hidden ? 'Polling pauses while this tab is hidden.' : `Refreshes every ${seconds} s; this is not a push channel.`}</TooltipContent>
    </Tooltip>
  )
}

const initials = (name) => (name ?? '?').trim().split(/\s+/).slice(0, 2).map((w) => [...w][0] ?? '').join('').toUpperCase()

/** True from 150 ms after a page visit starts (not polls, controls or partial reloads) until it finishes. */
function useNavigating() {
  const [navigating, setNavigating] = useState(false)
  useEffect(() => {
    let timer = 0
    const off = [
      router.on('start', (e) => {
        const v = e.detail.visit
        if (v.method === 'get' && !v.async && !(v.only?.length)) timer = setTimeout(() => setNavigating(true), 150)
      }),
      router.on('finish', () => { clearTimeout(timer); setNavigating(false) }),
    ]
    return () => { clearTimeout(timer); off.forEach((f) => f()) }
  }, [])
  return navigating
}

export function Layout({ heading, description, actions, live, children }) {
  const { component, props: { notice } } = usePage()
  const navigating = useNavigating()
  return (
    <SidebarProvider>
      <Sidebar label="Dashboard">
        <SidebarHeader><Brand /></SidebarHeader>
        <SidebarContent><Nav /></SidebarContent>
        <SidebarFooter><UserRow /></SidebarFooter>
      </Sidebar>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-30 flex h-14 items-center gap-2 border-b bg-background/80 px-3 backdrop-blur-md md:px-5">
          <SidebarTrigger />
          <span aria-hidden className="h-5 w-px bg-border" />
          <p className="min-w-0 truncate text-sm text-muted-foreground"><span className="hidden sm:inline">Runwick / </span><span className="font-medium text-foreground">{TITLES[component] ?? component}</span></p>
          <div className="flex-1" />
          <LiveIndicator seconds={live} />
          <ThemeToggle />
        </header>
        <main className="mx-auto grid w-full max-w-[96rem] min-w-0 flex-1 content-start gap-6 p-4 md:p-6">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div className="grid min-w-0 gap-1">
              <h1 className="truncate text-2xl font-semibold tracking-tight">{heading}</h1>
              {description && <p className="text-sm text-muted-foreground">{description}</p>}
            </div>
            {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
          </div>
          {notice && <p className="flex items-center gap-2 rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm" data-testid="notice">
            <IconAlertTriangle className="size-4 shrink-0 text-amber-600 dark:text-amber-400" />
            {notice === 'invalid_cursor' ? 'That page link expired or is not valid; showing the first page.' : <span>The filter was not accepted: <code>{notice}</code></span>}
          </p>}
          {navigating ? <PageSkeleton /> : <div key={component} className="grid min-w-0 animate-page-in gap-6">{children}</div>}
        </main>
      </div>
    </SidebarProvider>
  )
}

function PageSkeleton() {
  return (
    <div className="grid gap-4" aria-busy="true" aria-label="Loading">
      <div className="flex gap-3"><Skeleton className="h-9 w-48" /><Skeleton className="h-9 w-32" /></div>
      <Card className="gap-3 p-4">{Array.from({ length: 8 }, (_, i) => <Skeleton key={i} className="h-8 w-full" />)}</Card>
    </div>
  )
}

// ---- Small pieces used by the pages ---------------------------------------------------------------------------------------------

export const time = (value) => (value ? new Date(value).toLocaleString() : '—')

/** "as of …" and the Next link: keyset pages, no totals (16 §2). */
export function Pager({ page, cursorParam = 'cursor' }) {
  const { url } = usePage()
  const next = () => {
    const u = new URL(url, window.location.href)
    u.searchParams.set(cursorParam, page.next)
    return u.pathname + u.search
  }
  return (
    <div className="flex items-center justify-between gap-3 border-t px-4 pt-3 text-xs text-muted-foreground">
      <span data-testid="observed">as of {new Date(page.observedAt).toLocaleTimeString()}</span>
      {page.next && <Button asChild size="sm" variant="outline">
        <Link href={next()} data-testid="next" className="group">Next <IconArrowRight className="transition-transform group-hover:translate-x-0.5" /></Link>
      </Button>}
    </div>
  )
}

/**
 * A GET filter form: submits as an Inertia visit with the non-empty fields as the query. A field is [name, label] (text) or
 * [name, label, options] (a select; 'any' sends nothing).
 */
export function Filters({ fields, values }) {
  const { url } = usePage()
  const path = new URL(url, window.location.href).pathname
  const active = fields.some(([name]) => values?.[name])
  const submit = (e) => {
    e.preventDefault()
    const data = Object.fromEntries([...new FormData(e.target)].filter(([, v]) => v !== '' && v !== 'any'))
    router.get(path, data)
  }
  return (
    <form className="flex flex-wrap items-end gap-3" onSubmit={submit} data-testid="filters">
      {fields.map(([name, label, options]) => (
        <div key={name} className="grid gap-1.5">
          <Label htmlFor={`filter-${name}`} className="text-xs text-muted-foreground">{label}</Label>
          {options
            ? <Select name={name} defaultValue={values?.[name] ?? 'any'}>
              <SelectTrigger id={`filter-${name}`} className="h-8 min-w-36"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="any">Any</SelectItem>
                {options.map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}
              </SelectContent>
            </Select>
            : <Input id={`filter-${name}`} name={name} defaultValue={values?.[name] ?? ''} className="h-8 w-40"
              placeholder={name === 'from' || name === 'to' ? 'YYYY-MM-DDThh:mmZ' : 'Any'} />}
        </div>
      ))}
      <Button type="submit" size="sm"><IconFilter />Filter</Button>
      {active && <Button asChild size="sm" variant="ghost"><Link href={path}><IconX />Clear</Link></Button>}
    </form>
  )
}

/** Sorts the rows of the current page in the browser (pages come from the server in their own order; this never fetches). */
export function useSort(items, initial = null) {
  const [sort, setSort] = useState(initial)
  const rows = !sort ? items : [...items].sort((a, b) => {
    const x = sort.get(a), y = sort.get(b)
    const c = x == null ? 1 : y == null ? -1 : typeof x === 'number' ? x - y : String(x).localeCompare(String(y))
    return sort.desc ? -c : c
  })
  const head = (key, label, get, className) => {
    const on = sort?.key === key
    const Icon = !on ? IconArrowsSort : sort.desc ? IconArrowDown : IconArrowUp
    return (
      <TableHead className={className} aria-sort={on ? (sort.desc ? 'descending' : 'ascending') : undefined}>
        <button type="button" className="group -mx-1 inline-flex items-center gap-1 rounded px-1 uppercase outline-none hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50"
          onClick={() => setSort(on && sort.desc ? null : { key, get, desc: on })}>
          {label}<Icon className={cn('size-3.5 transition-opacity', on ? 'opacity-100' : 'opacity-0 group-hover:opacity-60 group-focus-visible:opacity-60')} />
        </button>
      </TableHead>
    )
  }
  return [rows, head]
}

/**
 * Keys whose value changed between two polls, for a short highlight. Each entry is a counter so a second change re-keys
 * the highlight and replays it.
 */
export function useChanged(items, key, value) {
  const prev = useRef(null)
  const [hot, setHot] = useState({})
  useEffect(() => {
    const seen = prev.current
    const now = Object.fromEntries(items.map((i) => [key(i), value(i)]))
    prev.current = now
    if (!seen) return
    const changed = Object.keys(now).filter((k) => k in seen && seen[k] !== now[k])
    if (!changed.length) return
    setHot((h) => ({ ...h, ...Object.fromEntries(changed.map((k) => [k, (h[k] ?? 0) + 1])) }))
    setTimeout(() => setHot((h) => Object.fromEntries(Object.entries(h).filter(([k]) => !changed.includes(k)))), 1700)
  }, [items]) // eslint-disable-line react-hooks/exhaustive-deps -- key and value are pure
  return hot
}

export { StatusBadge as StateBadge } from './components/status'

/** Copies a value; the icon turns into a check for a moment, and screen readers hear "Copied". */
export function CopyButton({ value, label = 'Copy' }) {
  const [copied, setCopied] = useState(false)
  if (!navigator.clipboard) return null
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch { /* the browser refused; nothing to confirm */ }
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button type="button" size="icon-sm" variant="ghost" onClick={copy} aria-label={label} className="size-7 text-muted-foreground">
          <span className="relative size-4">
            <IconCopy className={cn('absolute inset-0 size-4 transition-all duration-200', copied ? 'scale-50 opacity-0' : 'scale-100 opacity-100')} />
            <IconCheck className={cn('absolute inset-0 size-4 text-success transition-all duration-200', copied ? 'scale-100 opacity-100' : 'scale-50 opacity-0')} />
          </span>
        </Button>
      </TooltipTrigger>
      <TooltipContent>{copied ? 'Copied' : label}</TooltipContent>
      <span className="sr-only" aria-live="polite">{copied ? 'Copied' : ''}</span>
    </Tooltip>
  )
}

const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)')

/** A number that counts to its new value when a poll changes it (instantly with reduced motion). Non-numbers show as they are. */
export function AnimatedNumber({ value, className }) {
  const [shown, setShown] = useState(value)
  const from = useRef(value)
  useEffect(() => {
    if (typeof value !== 'number' || typeof from.current !== 'number' || reducedMotion.matches) {
      from.current = value
      setShown(value)
      return undefined
    }
    const start = performance.now(), a = from.current, b = value
    let frame = 0
    const step = (now) => {
      const t = Math.min((now - start) / 600, 1)
      const v = a + (b - a) * (1 - (1 - t) ** 3)
      from.current = v
      setShown(t < 1 ? v : b)
      if (t < 1) frame = requestAnimationFrame(step)
    }
    frame = requestAnimationFrame(step)
    return () => cancelAnimationFrame(frame)
  }, [value])
  return <span className={cn('tabular-nums', className)}>{typeof shown === 'number' ? Math.round(shown).toLocaleString() : shown}</span>
}

/**
 * A small area chart drawn as SVG, in the shadcn chart style (colour from a --chart-n token through currentColor). Recharts is
 * not used: its d3 dependencies are ISC, outside the dashboard's licence allow-list.
 */
export function Sparkline({ data, className, label }) {
  const points = data.length === 1 ? [data[0], data[0]] : data
  if (points.length === 0) return <div className={cn('h-10', className)} />
  const max = Math.max(...points), min = Math.min(...points), span = max - min || 1
  const xy = points.map((v, i) => [(i / (points.length - 1)) * 100, 36 - ((v - min) / span) * 32])
  const line = xy.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(2)},${y.toFixed(2)}`).join(' ')
  return (
    <svg viewBox="0 0 100 40" preserveAspectRatio="none" role="img" aria-label={label} className={cn('h-10 w-full overflow-visible', className)}>
      <path d={`${line} L100,40 L0,40 Z`} fill="currentColor" fillOpacity="0.12" className="transition-[d] duration-500" />
      <path d={line} fill="none" stroke="currentColor" strokeWidth="1.75" vectorEffect="non-scaling-stroke" strokeLinejoin="round" strokeLinecap="round" className="transition-[d] duration-500" />
    </svg>
  )
}

/** A table inside a card, with an empty state and the pager underneath. */
export function TableCard({ children, empty, pager, toolbar, title, description }) {
  return (
    <Card className="gap-0 overflow-hidden py-0">
      {(title || toolbar) && <div className="flex flex-wrap items-end justify-between gap-3 border-b p-4">
        {title && <div className="grid gap-1"><h2 className="font-semibold leading-none">{title}</h2>
          {description && <p className="text-sm text-muted-foreground">{description}</p>}</div>}
        {toolbar}
      </div>}
      {children}
      {empty && <p className="grid place-items-center gap-1 px-4 py-10 text-center text-sm text-muted-foreground">{empty}</p>}
      {pager && <div className="pb-3">{pager}</div>}
    </Card>
  )
}

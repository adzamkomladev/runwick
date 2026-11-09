import { Link, router, usePage } from '@inertiajs/react'
import {
  IconAlertTriangle, IconArrowDown, IconArrowRight, IconArrowUp, IconArrowsSort, IconBell, IconBolt, IconCalendarTime, IconCheck,
  IconCopy, IconDeviceDesktop, IconFilter, IconGauge, IconHistory, IconLayoutDashboard, IconListDetails, IconLoader2, IconLogout,
  IconMoon, IconPlayerPause, IconSelector, IconServer2, IconSettings, IconStack2, IconSun, IconTrash, IconX,
} from '@tabler/icons-react'
import { useEffect, useRef, useState, useSyncExternalStore } from 'react'
import {
  AlertDialog, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from './components/ui/alert-dialog'
import { Badge } from './components/ui/badge'
import { Button } from './components/ui/button'
import { Card } from './components/ui/card'
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuRadioGroup, DropdownMenuSegment, DropdownMenuSeparator, DropdownMenuTrigger,
} from './components/ui/dropdown-menu'
import { useQueryNav } from './components/date-range'
import { Combobox } from './components/ui/combobox'
import { Input } from './components/ui/input'
import { Label } from './components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './components/ui/select'
import { Sidebar, SidebarContent, SidebarFooter, SidebarGroup, SidebarHeader, SidebarMenuButton, SidebarProvider, SidebarTrigger, useSidebar } from './components/ui/sidebar'
import { Sheet, SheetContent, SheetDescription, SheetFooter, SheetHeader, SheetTitle, SheetTrigger } from './components/ui/sheet'
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
        reset: key.split(','), // a poll replaces a merge prop's first page instead of appending to it
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
 * One control at a time (21 §5): confirm, then post with a fresh command ID. Every result is a toast: applied, rejected, unknown
 * (with Resend: the same command ID and body, so it is applied at most once), no answer (Try again) and bulk results.
 */
export function useCommand() {
  const [state, setState] = useState(null)
  const send = (s) => {
    acting++
    dismissToast(RESULT_TOAST)
    setState({ ...s, status: 'sending' })
    const resend = (label) => ({ label, testId: 'again', onClick: () => send(s) })
    const failed = (code) => toast({ id: RESULT_TOAST, variant: 'error', testId: 'failed', duration: Infinity, title: `${s.label}: no answer`,
      description: <>Code <code data-testid="failed-code">{code}</code>. Nothing is known to have changed.</>, action: resend('Try again') })
    router.post(s.url, s.body, {
      headers: { 'X-Runwick-Csrf': csrf(), 'X-Runwick-Command-Id': s.commandId },
      preserveScroll: true,
      onSuccess: (page) => {
        const o = page.props.outcome
        if (!o) {
          toast({ id: RESULT_TOAST, variant: 'success', testId: 'done', title: s.label, description: s.done === 'done' ? 'Applied.' : s.done })
        } else if (o.items) {
          const bad = o.items.filter((i) => i.code)
          toast({ id: RESULT_TOAST, variant: bad.length ? 'warning' : 'success', testId: 'bulk', duration: bad.length ? Infinity : undefined, title: s.label,
            description: <span data-testid="bulk-summary">{o.items.length - bad.length} applied{bad.length > 0 && <>, {bad.length} not: {[...new Set(bad.map((i) => i.code))].map((c) => <code key={c} className="mr-1">{c}</code>)}</>}</span> })
        } else if (o.code === 'outcome_unknown') {
          toast({ id: RESULT_TOAST, variant: 'warning', testId: 'unknown', duration: Infinity, title: `${s.label}: result not known yet`,
            description: 'It may still be applied. Resend sends the same command, which is applied at most once.', action: resend('Resend') })
        } else if (['store_unavailable', 'query_budget_exceeded', 'busy'].includes(o.code)) {
          failed(o.code)
        } else {
          toast({ id: RESULT_TOAST, variant: 'error', testId: 'rejected', title: `${s.label}: not applied`,
            description: <>Code <code>{o.code}</code>{o.actualRevision != null && <> (now at revision {o.actualRevision})</>}. The page shows the current state.</> })
        }
        setState(null)
      },
      onHttpException: (response) => { failed(response.data?.code ?? `http_${response.status}`); setState(null); return false },
      onNetworkError: () => { failed('no_answer'); setState(null); return false },
      onFinish: () => { acting-- },
    })
  }
  return {
    state,
    ask: (label, url, body, done = 'done') => setState({ label, url, body, done, status: 'confirm' }),
    /** Sends at once, without the confirm dialog (a form in a sheet is its own confirmation). */
    run: (label, url, body, done = 'done') => send({ label, url, body, done, commandId: crypto.randomUUID() }),
    confirm: () => send({ ...state, commandId: crypto.randomUUID() }),
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

/** The confirm dialog. Results arrive as toasts (useCommand). */
export function CommandBar({ command }) {
  const s = command.state
  return (
    <AlertDialog open={s?.status === 'confirm'} onOpenChange={(open) => { if (!open) command.close() }}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{s?.label}</AlertDialogTitle>
          <AlertDialogDescription>Checked against the revision this page shows.</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Back</AlertDialogCancel>
          <Button data-testid="confirm" onClick={command.confirm}>Confirm</Button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
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

const THEMES = [['light', 'Light', IconSun], ['system', 'System', IconDeviceDesktop], ['dark', 'Dark', IconMoon]]

// ---- Notifications: recent failures, scaling changes and paused queues since the last look (unread state in this browser). ----

const SEEN_KEY = 'runwick-dashboard-seen'
const readSeen = () => { try { return JSON.parse(localStorage.getItem(SEEN_KEY)) ?? {} } catch { return {} } }

function useNotifications() {
  const { notifications: n } = usePage().props
  useEffect(() => { // the prop is deferred and kept for a minute; refresh it each minute while the tab is visible
    const t = setInterval(() => { if (!document.hidden && acting === 0) router.reload({ only: ['notifications'], async: true, onHttpException: () => false, onNetworkError: () => false }) }, 60_000)
    return () => clearInterval(t)
  }, [])
  const [seen, setSeen] = useState(readSeen)
  const at = seen.at ? Date.parse(seen.at) : 0
  const items = [
    ...(n?.failed ?? []).map((f) => ({ key: `f${f.jobId}`, kind: 'failed', at: f.at, title: f.jobName, text: `failed on ${f.queue}`, href: `/jobs/${f.jobId}` })),
    ...(n?.scaling ?? []).map((e) => ({ key: `s${e.eventId}`, kind: 'scaling', at: e.at, title: e.pool, text: `target ${e.oldTarget} → ${e.newTarget}`, href: '/scaling' })),
    ...(n?.paused ?? []).map((q) => ({ key: `p${q.name}@${q.revision}`, kind: 'paused', at: null, title: q.name, text: 'queue paused', href: '/queues' })),
  ].sort((a, b) => (b.at ? Date.parse(b.at) : Infinity) - (a.at ? Date.parse(a.at) : Infinity))
  const unread = (i) => (i.at ? Date.parse(i.at) > at : !(seen.paused ?? []).includes(i.key))
  const markSeen = () => {
    const next = { at: new Date().toISOString(), paused: items.filter((i) => i.kind === 'paused').map((i) => i.key) }
    try { localStorage.setItem(SEEN_KEY, JSON.stringify(next)) } catch { /* not remembered */ }
    setSeen(next)
  }
  return { loaded: n !== undefined, items, unread, count: items.filter(unread).length, markSeen }
}

const KIND = { failed: [IconAlertTriangle, 'text-destructive'], scaling: [IconGauge, 'text-primary'], paused: [IconPlayerPause, 'text-warning'] }

/** The notifications list, in a sheet opened from the account menu; closing it marks everything seen. */
function NotificationsSheet({ open, onOpenChange, notes }) {
  const { base } = usePage().props
  const { loaded, items, unread, markSeen } = notes
  return (
    <Sheet open={open} onOpenChange={(o) => { onOpenChange(o); if (!o) markSeen() }}>
      <SheetContent side="right" className="gap-0 p-0" data-testid="notifications-panel">
        <SheetHeader className="border-b"><SheetTitle>Notifications</SheetTitle><SheetDescription>The last 24 hours</SheetDescription></SheetHeader>
        {!loaded ? <div className="grid gap-2 p-3">{[0, 1, 2].map((i) => <Skeleton key={i} className="h-12" />)}</div>
          : items.length === 0 ? <p className="px-3 py-12 text-center text-sm text-muted-foreground">Nothing new.</p>
            : <ul className="min-h-0 flex-1 overflow-y-auto p-2">{items.map((i) => {
              const [Icon, tone] = KIND[i.kind]
              return (
                <li key={i.key}>
                  <Link href={base + i.href} onClick={() => onOpenChange(false)} className="flex items-start gap-3 rounded-md px-2.5 py-2.5 text-sm outline-none hover:bg-accent focus-visible:ring-[3px] focus-visible:ring-ring/50">
                    <Icon aria-hidden className={cn('mt-0.5 size-4 shrink-0', tone)} />
                    <span className="grid min-w-0 flex-1"><span className="truncate font-medium">{i.title}</span><span className="text-xs text-muted-foreground">{i.text}{i.at && <> · {relativeTime(i.at)}</>}</span></span>
                    {unread(i) && <span aria-label="new" className="mt-1.5 size-1.5 shrink-0 rounded-full bg-highlight ring-2 ring-highlight/30" />}
                  </Link>
                </li>
              )
            })}</ul>}
      </SheetContent>
    </Sheet>
  )
}

const relativeTime = (iso) => {
  const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000)
  return s < 60 ? 'just now' : s < 3600 ? `${Math.round(s / 60)} min ago` : `${Math.round(s / 3600)} h ago`
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
      <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-midnight-violet-950 text-highlight ring-1 ring-midnight-violet-800 dark:bg-highlight dark:text-midnight-violet-950 dark:ring-0">
        <IconBolt className="size-4.5" aria-hidden />
      </span>
      <span className="truncate font-semibold tracking-tight transition-opacity duration-200 group-data-[sidebar=collapsed]/sidebar:sr-only" data-testid="title">{title}</span>
    </div>
  )
}

const Avatar = ({ name, className }) => (
  <span aria-hidden className={cn('grid size-8 shrink-0 place-items-center rounded-lg bg-primary/15 text-xs font-semibold text-primary dark:bg-primary/20', className)}>{initials(name)}</span>
)

/**
 * The footer: one account button (who is signed in) that opens a menu with notifications, settings, the theme switch and
 * sign out. In the icon rail only the avatar shows; a dot on it says there are unread notifications.
 */
function Footer() {
  const { user, userDetail, signOut, can, base } = usePage().props
  const { open, mobile } = useSidebar()
  const theme = useSyncExternalStore((l) => { themeListeners.add(l); return () => themeListeners.delete(l) }, readTheme)
  const notes = useNotifications()
  const [sheet, setSheet] = useState(false)
  const rail = !open && !mobile
  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button type="button" data-testid="account" aria-label={notes.count ? `Account, ${notes.count} new notifications` : 'Account'}
            className={cn('flex w-full min-w-0 items-center gap-2.5 rounded-lg bg-sidebar-accent/60 p-1.5 text-left outline-none ring-1 ring-sidebar-border transition-colors hover:bg-sidebar-accent focus-visible:ring-[3px] focus-visible:ring-ring/50 data-[state=open]:bg-sidebar-accent', rail && 'justify-center bg-transparent p-1 ring-0')}>
            <span className="relative">
              <Avatar name={user} />
              {notes.count > 0 && <span data-testid="unread" className="absolute -top-1 -right-1 size-2.5 rounded-full bg-highlight ring-2 ring-sidebar" />}
            </span>
            <span className={cn('grid min-w-0 flex-1 leading-tight', rail && 'sr-only')}>
              <span className="truncate text-sm font-medium" data-testid="user">{user}</span>
              {userDetail && <span className="truncate text-xs text-muted-foreground">{userDetail}</span>}
            </span>
            {!rail && <IconSelector aria-hidden className="size-4 shrink-0 text-muted-foreground" />}
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent side={mobile ? 'top' : 'right'} align="end" sideOffset={mobile ? 6 : 10} className="w-64 rounded-xl p-0 shadow-lg" data-testid="account-menu">
          <div className="flex items-center gap-2.5 p-2.5">
            <Avatar name={user} />
            <span className="grid min-w-0 leading-tight"><span className="truncate text-sm font-semibold">{user}</span>
              {userDetail && <span className="truncate text-xs text-muted-foreground">{userDetail}</span>}</span>
          </div>
          <DropdownMenuSeparator className="mx-0 my-0" />
          <div className="p-1">
            <DropdownMenuItem data-testid="notifications" onSelect={() => setSheet(true)}>
              <IconBell />Notifications
              {notes.count > 0 && <span className="ml-auto grid h-5 min-w-5 place-items-center rounded-full bg-highlight px-1.5 text-[0.6875rem] font-semibold text-highlight-foreground tabular-nums">{notes.count > 9 ? '9+' : notes.count}</span>}
            </DropdownMenuItem>
            {can.settings && <DropdownMenuItem asChild><Link href={`${base}/settings`} data-testid="account-settings"><IconSettings />Settings</Link></DropdownMenuItem>}
          </div>
          <DropdownMenuSeparator className="mx-0 my-0" />
          <div className="flex items-center justify-between gap-2 py-1.5 pr-1.5 pl-3">
            <span className="text-sm">Theme</span>
            <DropdownMenuRadioGroup value={theme} onValueChange={setTheme} aria-label="Theme" className="flex items-center gap-0.5 rounded-full bg-muted p-0.5" data-testid="theme">
              {THEMES.map(([v, label, I]) => <DropdownMenuSegment key={v} value={v} aria-label={label} title={label} data-testid={`theme-${v}`} onSelect={(e) => e.preventDefault()}><I /></DropdownMenuSegment>)}
            </DropdownMenuRadioGroup>
          </div>
          {signOut && <>
            <DropdownMenuSeparator className="mx-0 my-0" />
            <form method="post" action={signOut} className="p-1">
              <DropdownMenuItem asChild><button type="submit" className="w-full" data-testid="sign-out"><IconLogout />Log out</button></DropdownMenuItem>
            </form>
          </>}
        </DropdownMenuContent>
      </DropdownMenu>
      <NotificationsSheet open={sheet} onOpenChange={setSheet} notes={notes} />
    </>
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
          <span className={cn('size-2 rounded-full', hidden ? 'bg-muted-foreground/50' : 'animate-live bg-highlight')} />
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
        <SidebarFooter><Footer /></SidebarFooter>
      </Sidebar>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-30 flex h-14 items-center gap-2 border-b bg-background/80 px-3 backdrop-blur-md md:px-5">
          <SidebarTrigger />
          <span aria-hidden className="h-5 w-px bg-border" />
          <p className="min-w-0 truncate text-sm text-muted-foreground"><span className="hidden sm:inline">Runwick / </span><span className="font-medium text-foreground">{TITLES[component] ?? component}</span></p>
          <div className="flex-1" />
          <LiveIndicator seconds={live} />
        </header>
        <main className="mx-auto grid w-full max-w-[96rem] min-w-0 flex-1 content-start gap-6 p-4 md:p-6">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div className="grid min-w-0 gap-1">
              <h1 className="truncate text-2xl font-semibold tracking-tight">{heading}</h1>
              {description && <p className="text-sm text-muted-foreground">{description}</p>}
            </div>
            {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
          </div>
          {notice && <p className="flex items-center gap-2 rounded-lg border border-warning/40 bg-warning/5 p-3 text-sm" data-testid="notice">
            <IconAlertTriangle className="size-4 shrink-0 text-warning" />
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
 * Filters in a side sheet (21 §4.2): a "Filters" button showing how many are set, and a sheet with one control per field.
 * A field is { name, label, options?, free? }: options make it a select (or, with `free`, a searchable combobox that also takes
 * typed text); none makes it a text field. Apply visits the page with the set fields as the query (other query values, such as
 * the date range, are kept); Clear removes them all.
 */
export function FilterSheet({ fields, values }) {
  const go = useQueryNav()
  const [open, setOpen] = useState(false)
  const [draft, setDraft] = useState({})
  const count = fields.filter((f) => values?.[f.name]).length
  const set = (name, v) => setDraft((d) => ({ ...d, [name]: v }))
  const apply = (e) => {
    e.preventDefault()
    setOpen(false)
    go(Object.fromEntries(fields.map((f) => [f.name, draft[f.name] && draft[f.name] !== 'any' ? draft[f.name] : null])))
  }
  return (
    <Sheet open={open} onOpenChange={(o) => { setOpen(o); if (o) setDraft(Object.fromEntries(fields.map((f) => [f.name, values?.[f.name] ?? '']))) }}>
      <div className="flex items-center gap-1">
        <SheetTrigger asChild>
          <Button variant="outline" size="sm" data-testid="filters"><IconFilter />Filters{count > 0 && <Badge variant="default" className="h-5 min-w-5 justify-center rounded-full px-1">{count}</Badge>}</Button>
        </SheetTrigger>
        {count > 0 && <Button variant="ghost" size="icon-sm" aria-label="Clear filters" data-testid="clear-filters" onClick={() => go(Object.fromEntries(fields.map((f) => [f.name, null])))}><IconX /></Button>}
      </div>
      <SheetContent data-testid="filter-sheet">
        <SheetHeader><SheetTitle>Filters</SheetTitle><SheetDescription className="sr-only">Narrow the list</SheetDescription></SheetHeader>
        <form onSubmit={apply} className="flex min-h-0 flex-1 flex-col">
          <div className="grid flex-1 content-start gap-4 overflow-y-auto px-5">
            {fields.map((f) => (
              <div key={f.name} className="grid gap-1.5">
                <Label htmlFor={`filter-${f.name}`}>{f.label}</Label>
                {f.options && f.free
                  ? <Combobox id={`filter-${f.name}`} label={f.label} value={draft[f.name] ?? ''} onChange={(v) => set(f.name, v)} options={f.options} free />
                  : f.options
                    ? <Select value={draft[f.name] || 'any'} onValueChange={(v) => set(f.name, v)}>
                      <SelectTrigger id={`filter-${f.name}`} data-testid={`filter-${f.name}`} className="w-full"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="any">Any</SelectItem>
                        {f.options.map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}
                      </SelectContent>
                    </Select>
                    : <Input id={`filter-${f.name}`} data-testid={`filter-${f.name}`} value={draft[f.name] ?? ''} placeholder="Any" onChange={(e) => set(f.name, e.target.value)} />}
              </div>
            ))}
          </div>
          <SheetFooter>
            <Button type="button" variant="ghost" onClick={() => setDraft(Object.fromEntries(fields.map((f) => [f.name, ''])))}>Reset</Button>
            <Button type="submit" data-testid="apply-filters">Apply</Button>
          </SheetFooter>
        </form>
      </SheetContent>
    </Sheet>
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

async function copy(value) {
  try {
    await navigator.clipboard.writeText(value)
    toast({ id: 'copied', variant: 'copied', testId: 'copied', title: 'Copied' })
    return true
  } catch { return false } // the browser refused; nothing to confirm
}

/** Copies a value; the icon turns into a check for a moment and a "Copied" toast shows (also read out). */
export function CopyButton({ value, label = 'Copy', className }) {
  const [copied, setCopied] = useState(false)
  if (!navigator.clipboard) return null
  const onClick = async (e) => {
    e.preventDefault()
    e.stopPropagation()
    if (await copy(value)) { setCopied(true); setTimeout(() => setCopied(false), 1500) }
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button type="button" onClick={onClick} aria-label={label} data-testid="copy"
          className={cn('inline-grid size-6 shrink-0 place-items-center rounded-md text-muted-foreground outline-none transition-colors hover:bg-accent hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50', className)}>
          <span className="relative size-3.5">
            <IconCopy className={cn('absolute inset-0 size-3.5 transition-all duration-200', copied ? 'scale-50 opacity-0' : 'scale-100 opacity-100')} />
            <IconCheck className={cn('absolute inset-0 size-3.5 text-success transition-all duration-200', copied ? 'scale-100 opacity-100' : 'scale-50 opacity-0')} />
          </span>
        </button>
      </TooltipTrigger>
      <TooltipContent>{copied ? 'Copied' : label}</TooltipContent>
    </Tooltip>
  )
}

/** A value (ID, error code, queue or job name) with a copy button that appears on hover or keyboard focus. */
export function Copyable({ value, children, label, className }) {
  return (
    <span className={cn('group/copy inline-flex max-w-full min-w-0 items-center gap-1', className)}>
      <span className="min-w-0 truncate">{children ?? value}</span>
      <CopyButton value={String(value)} label={label ?? 'Copy'} className="opacity-0 transition-opacity group-hover/copy:opacity-100 focus-visible:opacity-100 pointer-coarse:opacity-100" />
    </span>
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

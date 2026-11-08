import { Link, router, usePage } from '@inertiajs/react'
import { useEffect, useState } from 'react'

// The antiforgery token is in the HTML <meta>, never in page props, so it never enters history state.
const csrf = () => document.querySelector('meta[name="runwick-csrf"]')?.content ?? ''

// Controls and reveals in flight on this page: polls wait so a refreshed revision cannot change under a submitted command.
let acting = 0

const MAX_BACKOFF = 60_000

/**
 * Polls the page's list props (21 §4.1): a partial reload of `only`, first page only (no cursor in the URL), one request in
 * flight, paused while the tab is hidden (one poll on becoming visible), and the interval doubles to 60 s on failure.
 */
export function usePoll(only, seconds) {
  const { url } = usePage()
  const key = only.join(',')
  useEffect(() => {
    if (!seconds || new URL(url, window.location.href).searchParams.has('cursor')) return undefined
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
  }, [url, key, seconds])
}

/**
 * One control at a time (21 §5): confirm, then post with a fresh command ID. "Try again" and "Check again" resend the same
 * command ID and body. Success is shown from the request's callback, not from a prop.
 */
export function useCommand() {
  const [state, setState] = useState(null)
  const send = (s) => {
    acting++
    setState({ ...s, status: 'sending' })
    router.post(s.url, s.body, {
      headers: { 'X-Runwick-Csrf': csrf(), 'X-Runwick-Command-Id': s.commandId },
      preserveScroll: true,
      onSuccess: (page) => {
        const outcome = page.props.outcome
        const status = !outcome ? 'done' : outcome.items ? 'bulk' : outcome.code === 'outcome_unknown' ? 'unknown'
          : ['store_unavailable', 'query_budget_exceeded', 'busy'].includes(outcome.code) ? 'failed' : 'rejected'
        setState({ ...s, status, outcome })
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
  }
}

export function CommandBar({ command }) {
  const s = command.state
  if (!s) return null
  const o = s.outcome
  return (
    <div className="notice" data-testid="command" data-status={s.status}>
      <strong>{s.label}</strong>{' '}
      {s.status === 'confirm' && <>
        <span>Are you sure?</span>{' '}
        <button data-testid="confirm" onClick={command.confirm}>Confirm</button>{' '}
        <button onClick={command.close}>Back</button>
      </>}
      {s.status === 'sending' && <span>Sending…</span>}
      {s.status === 'done' && <span data-testid="done">{s.done}</span>}
      {s.status === 'rejected' && <span data-testid="rejected">
        Not applied: <code>{o.code}</code>{o.actualRevision != null && <> (current revision {o.actualRevision})</>}. The page shows the current state.
      </span>}
      {s.status === 'unknown' && <span>
        The result is not known yet (command <code>{o.commandId}</code>).{' '}
        <button data-testid="again" onClick={command.again}>Check again</button>
      </span>}
      {s.status === 'failed' && <span>
        No result: <code data-testid="failed-code">{o.code}</code>.{' '}
        <button data-testid="again" onClick={command.again}>Try again</button>
      </span>}
      {s.status === 'bulk' && <ul data-testid="bulk">
        {o.items.map((i) => <li key={i.jobId}><code>{i.jobId}</code>: {i.status}{i.code && <> ({i.code})</>}</li>)}
      </ul>}
      {s.status !== 'confirm' && s.status !== 'sending' && <> <button onClick={command.close}>Dismiss</button></>}
    </div>
  )
}

/** A reveal (21 §5.1): plain fetch, the value kept in component state only and cleared on navigation. */
export function useReveal() {
  const [value, setValue] = useState(null)
  useEffect(() => { // 'success' also covers the 303 that follows a command on the same page
    const off = [router.on('navigate', () => setValue(null)), router.on('success', () => setValue(null))]
    return () => off.forEach((f) => f())
  }, [])
  const reveal = async (key, url) => {
    acting++
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
    }
  }
  return { value, reveal, clear: () => setValue(null) }
}

const NAV = [['jobs', 'Jobs', ''], ['queues', 'Queues', '/queues'], ['workers', 'Workers', '/workers'], ['schedules', 'Schedules', '/schedules'],
  ['scaling', 'Scaling', '/scaling'], ['settings', 'Settings', '/settings'], ['retention', 'Retention', '/retention'], ['audit', 'Audit', '/audit']]

export function Layout({ heading, children }) {
  const { base, title, user, can, notice } = usePage().props
  return (
    <>
      <header>
        <span className="brand" data-testid="title">{title}</span>
        <nav data-testid="nav">
          {NAV.filter(([page]) => can[page]).map(([page, label, path]) =>
            <Link key={page} href={base + path} data-testid={`nav-${page}`}>{label}</Link>)}
        </nav>
        <span className="user">Signed in as <span data-testid="user">{user}</span></span>
      </header>
      <main>
        <h1>{heading}</h1>
        {notice && <p className="notice" data-testid="notice">
          {notice === 'invalid_cursor' ? 'That page link expired or is not valid; showing the first page.' : <>The filter was not accepted: <code>{notice}</code></>}
        </p>}
        {children}
      </main>
    </>
  )
}

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
    <p className="pager">
      <span data-testid="observed">as of {new Date(page.observedAt).toLocaleTimeString()}</span>
      {page.next && <> · <Link href={next()} data-testid="next">Next</Link></>}
    </p>
  )
}

/** A GET filter form: submits as an Inertia visit with the non-empty fields as the query. */
export function Filters({ fields, values }) {
  const { url } = usePage()
  const submit = (e) => {
    e.preventDefault()
    const data = Object.fromEntries([...new FormData(e.target)].filter(([, v]) => v !== ''))
    router.get(new URL(url, window.location.href).pathname, data)
  }
  return (
    <form className="filters" onSubmit={submit} data-testid="filters">
      {fields.map(([name, label]) => (
        <label key={name}>{label} <input name={name} defaultValue={values?.[name] ?? ''} /></label>
      ))}
      <button type="submit">Filter</button>
    </form>
  )
}


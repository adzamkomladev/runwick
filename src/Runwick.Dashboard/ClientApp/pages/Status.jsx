import { Link } from '@inertiajs/react'
import { IconArrowLeft, IconCloudOff, IconLock, IconLogout, IconMapSearch } from '@tabler/icons-react'
import { Button } from '../components/ui/button'
import { Copyable, Layout } from '../lib'

// A page visit the dashboard could not serve (21 §3.7): no access, nothing there, or the store not answering. The shell stays, so
// the reader can go anywhere the navigation offers; the main action is the nearest page they can read.

const HOME = [['overview', '/overview', 'Overview'], ['runs', '/runs', 'Runs'], ['jobs', '/jobs', 'Jobs'], ['queues', '/queues', 'Queues'],
  ['workers', '/workers', 'Workers'], ['schedules', '/schedules', 'Schedules'], ['audit', '/audit', 'Audit'], ['settings', '/settings', 'Settings']]

const COPY = {
  403: { icon: IconLock, title: 'You don’t have access to this page', text: 'Your account is signed in, but its Runwick grants don’t include this page. Ask whoever manages access to add the permission below, or sign in with another account.' },
  404: { icon: IconMapSearch, title: 'Nothing here', text: 'This page doesn’t exist, or what it shows was removed or is outside the queues you can see.' },
  503: { icon: IconCloudOff, title: 'Runwick can’t reach its store', text: 'The dashboard is running, but the store did not answer in time. Your data is safe; try again in a moment.' },
}

export default function Status({ base, can, signOut, status: s }) {
  const copy = COPY[s.status] ?? COPY[404]
  const Icon = copy.icon
  // The section the missing or refused page belongs to (a run's 404 goes back to Runs), else the first page the caller can read.
  const home = HOME.find(([key, path]) => can[key] && s.status !== 403 && (s.path === path || s.path.startsWith(`${path}/`))) ?? HOME.find(([key]) => can[key])
  return (
    <Layout heading={copy.title}>
      <section data-testid="status-page" data-status={s.status} className="grid max-w-xl gap-6 pt-2 pb-6">
        <span className="grid size-12 place-items-center rounded-xl bg-muted text-muted-foreground ring-1 ring-border"><Icon className="size-6" aria-hidden /></span>
        <div className="grid gap-2">
          <p className="text-sm text-muted-foreground"><span className="font-mono tabular-nums">{s.status}</span> · <code>{s.code}</code></p>
          <p className="text-pretty text-base leading-relaxed">{copy.text}</p>
        </div>
        <dl className="grid gap-3 rounded-xl border bg-card p-4 text-sm">
          <div className="grid gap-1"><dt className="text-xs text-muted-foreground">Page</dt><dd className="min-w-0 break-all"><Copyable value={`${base}${s.path}`} label="Copy page path"><code>{base}{s.path}</code></Copyable></dd></div>
          {s.permission && <div className="grid gap-1"><dt className="text-xs text-muted-foreground">Permission needed</dt>
            <dd><Copyable value={s.permission} label="Copy permission name"><code data-testid="status-permission">{s.permission}</code></Copyable></dd></div>}
        </dl>
        <div className="flex flex-wrap gap-2">
          {home && <Button asChild data-testid="status-home"><Link href={`${base}${home[1]}`}><IconArrowLeft />Go to {home[2]}</Link></Button>}
          {s.status === 503 && <Button variant="outline" data-testid="status-retry" onClick={() => window.location.reload()}>Try again</Button>}
          {s.status === 403 && signOut && <form method="post" action={signOut}><Button type="submit" variant="outline" data-testid="status-sign-out"><IconLogout />Sign in as someone else</Button></form>}
        </div>
      </section>
    </Layout>
  )
}

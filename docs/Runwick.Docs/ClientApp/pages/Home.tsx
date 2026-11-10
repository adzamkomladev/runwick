import { Link } from '@inertiajs/react'
import { ArrowRight, Database, FileStack, Zap } from 'lucide-react'
import { useState, type ComponentProps } from 'react'
import { GithubIcon } from '../components/icons/GithubIcon'
import { InstallCommand } from '../components/InstallCommand'
import { Seo } from '../components/Seo'
import { SiteHeader } from '../components/SiteHeader'
import { cn } from '../lib/utils'
import DispatchSnippet from '../snippets/home-dispatch.mdx'
import JobSnippet from '../snippets/home-job.mdx'
import ProgramSnippet from '../snippets/home-program.mdx'

const stores = [
  {
    name: 'PostgreSQL',
    package: 'Runwick.Postgres',
    Icon: Database,
    fit: 'The reference store. Producers and workers on many hosts, and jobs that commit inside your own transaction.',
  },
  {
    name: 'SQLite',
    package: 'Runwick.Sqlite',
    Icon: FileStack,
    fit: 'One file on one machine. Durable writes, no server to run, and the same transactional completion.',
  },
  {
    name: 'Redis',
    package: 'Runwick.Redis',
    Icon: Zap,
    fit: 'One durable primary with append-only fsync. Many hosts; use the outbox to tie jobs to business data.',
  },
]

const reasons = [
  {
    title: 'It says what it promises',
    body: 'Delivery is at-least-once, cancellation is cooperative, and the docs list what Runwick does not claim. The one exactly-once case is completing a job in the same transaction as your own work.',
  },
  {
    title: 'One API, three stores',
    body: 'Jobs, handlers and registration are the same everywhere. Moving from SQLite on a laptop to PostgreSQL in production changes one line.',
  },
  {
    title: 'Operations are part of the library',
    body: 'A management HTTP API, a web dashboard and a runwick command line share one permission model and one audit trail. There is no anonymous mode.',
  },
  {
    title: 'Observable from day one',
    body: 'OpenTelemetry traces and metrics with fixed names, structured logs, health checks per role, a Grafana dashboard and Prometheus alert rules in the box.',
  },
]

const steps = [
  {
    title: 'Dispatch',
    body: 'Your app calls DispatchAsync. It returns only after the store has confirmed the write, so a job you were told about is a job that exists.',
  },
  {
    title: 'Store',
    body: 'The job waits in PostgreSQL, SQLite or Redis with its queue, priority, schedule and limits. Nothing lives only in memory.',
  },
  {
    title: 'Run',
    body: 'A worker, in your app or a separate process, leases the job, runs your handler and records the outcome. Failures retry with backoff.',
  },
]

// One job's real status history: the shape the dashboard shows, with the failure left in.
const history = [
  { state: 'pending', tone: 'muted', time: '09:14:02.118', note: 'Dispatched. The store confirmed the write.' },
  { state: 'running', tone: 'primary', time: '09:14:02.164', note: 'Attempt 1 on worker web-2' },
  { state: 'retrying', tone: 'warning', time: '09:14:03.902', note: 'Billing API answered 503. Next attempt in about 5 s.' },
  { state: 'running', tone: 'primary', time: '09:14:09.217', note: 'Attempt 2 on worker web-1, same idempotency key' },
  { state: 'succeeded', tone: 'success', time: '09:14:09.629', note: 'One invoice sent. The retry did not send a second.' },
] as const

const toneDot = {
  muted: 'bg-muted-foreground/50',
  primary: 'bg-primary',
  warning: 'bg-warning',
  success: 'bg-highlight ring-2 ring-success/40',
} as const

const toneText = {
  muted: 'text-muted-foreground',
  primary: 'text-primary',
  warning: 'text-warning',
  success: 'text-success',
} as const

function JobHistory() {
  return (
    <figure className="rounded-xl border border-border bg-card p-5 shadow-lift sm:p-6">
      <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 border-b border-border pb-4">
        <p className="font-mono text-sm font-medium">invoice.send</p>
        <p className="font-mono text-xs text-muted-foreground">2 attempts · max 3</p>
      </div>
      <ol className="mt-5 space-y-0">
        {history.map(({ state, tone, time, note }, index) => (
          <li key={time} className="relative grid grid-cols-[1rem_1fr] gap-x-3 pb-5 last:pb-0">
            {index < history.length - 1 && <span aria-hidden className="absolute bottom-0 left-[7px] top-4 w-px bg-border" />}
            <span aria-hidden className={cn('relative mt-1.5 size-[9px] justify-self-center rounded-full', toneDot[tone])} />
            <div className="min-w-0">
              <p className="flex flex-wrap items-baseline gap-x-3 font-mono text-xs">
                <span className={cn('font-semibold', toneText[tone])}>{state}</span>
                <span className="text-muted-foreground tabular-nums">{time}</span>
              </p>
              <p className="mt-1 text-sm leading-6 text-[color-mix(in_oklab,var(--foreground)_88%,var(--background))]">{note}</p>
            </div>
          </li>
        ))}
      </ol>
      <figcaption className="mt-5 border-t border-border pt-4 text-xs leading-5 text-muted-foreground">
        At-least-once means attempt 2 can happen. The idempotency key is what makes it harmless.
      </figcaption>
    </figure>
  )
}

const included = [
  ['Schedules and recurring jobs', '/docs/schedules'],
  ['Retries, backoff and timeouts', '/docs/retries-and-timeouts'],
  ['Uniqueness and rate limits', '/docs/uniqueness'],
  ['Queues, priorities and pausing', '/docs/queues-and-priorities'],
  ['Transactional completion', '/docs/transactions'],
  ['Transactional outbox', '/docs/outbox'],
  ['Worker pools and autoscaling', '/docs/workers'],
  ['Health checks per role', '/docs/health-checks'],
] as const

// Highlighted at build time by the same Shiki setup as the docs pages.
const code = {
  'Jobs.cs': JobSnippet,
  'Program.cs': ProgramSnippet,
  'Endpoints.cs': DispatchSnippet,
}
type CodeTab = keyof typeof code
const snippetComponents = { pre: (props: ComponentProps<'pre'>) => <pre {...props} className={cn(props.className, 'm-0 min-h-[236px] overflow-visible py-4 text-[12px]!')} /> }

function CodePreview() {
  const [tab, setTab] = useState<CodeTab>('Jobs.cs')
  const Snippet = code[tab]

  return (
    <div className="overflow-hidden rounded-xl border border-border bg-code shadow-lift">
      <div role="tablist" aria-label="Example files" className="flex border-b border-border bg-muted/50 text-xs">
        {(Object.keys(code) as CodeTab[]).map((value) => (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={tab === value}
            onClick={() => setTab(value)}
            className={cn(
              '-mb-px border-b-2 px-4 py-2.5 font-mono transition-colors',
              tab === value ? 'border-primary text-foreground' : 'border-transparent text-muted-foreground hover:text-foreground',
            )}
          >
            {value}
          </button>
        ))}
      </div>
      <div role="tabpanel" className="scrollbar-thin overflow-x-auto overflow-y-hidden">
        <Snippet components={snippetComponents} />
      </div>
    </div>
  )
}

function Footer() {
  return (
    <footer className="border-t border-border">
      <div className="mx-auto flex max-w-[1100px] flex-col gap-4 px-6 py-8 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
        <p>Runwick is MIT licensed.</p>
        <ul className="flex flex-wrap gap-x-5 gap-y-2">
          <li><Link href="/docs/introduction" className="hover:text-foreground">Docs</Link></li>
          <li><a href="https://github.com/adzamkomladev/runwick" target="_blank" rel="noreferrer" className="hover:text-foreground">GitHub</a></li>
          <li><a href="https://www.nuget.org/packages/Runwick" target="_blank" rel="noreferrer" className="hover:text-foreground">NuGet</a></li>
          <li><a href="https://github.com/adzamkomladev/runwick/tree/main/samples" target="_blank" rel="noreferrer" className="hover:text-foreground">Samples</a></li>
        </ul>
      </div>
    </footer>
  )
}

export default function Home() {
  return (
    <div className="min-h-screen">
      <Seo
        title="Runwick — background jobs for .NET"
        description="Runwick is a background-job library for .NET 10 on PostgreSQL, SQLite or Redis, with schedules, retries, a dashboard and honest delivery guarantees."
        type="website"
      />
      <SiteHeader />

      <main>
        <section className="border-b border-border">
          <div className="mx-auto grid max-w-[1100px] items-center gap-12 px-6 py-16 lg:grid-cols-[1fr_1fr] lg:py-24">
            <div>
              <h1 className="max-w-xl text-balance text-4xl font-semibold tracking-tight sm:text-5xl">
                Background jobs for .NET that say what they promise
              </h1>
              <p className="mt-5 max-w-lg text-pretty text-base leading-7 text-muted-foreground">
                Write a job to PostgreSQL, SQLite or Redis. A worker in your app, or in its own process, picks it up and runs your handler. Schedules, retries, rate limits, a dashboard and a CLI come with it.
              </p>
              <div className="mt-8 flex flex-wrap items-center gap-3">
                <Link
                  href="/docs/quick-start"
                  className="inline-flex h-10 items-center gap-2 rounded-md bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90"
                >
                  Get started
                  <ArrowRight className="size-4" />
                </Link>
                <a
                  href="https://github.com/adzamkomladev/runwick"
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex h-10 items-center gap-2 rounded-md border border-border px-4 text-sm font-medium transition-colors hover:bg-accent"
                >
                  <GithubIcon className="size-4" />
                  GitHub
                </a>
              </div>
              <div className="mt-6">
                <InstallCommand command="dotnet add package Runwick" />
              </div>
            </div>
            <CodePreview />
          </div>
        </section>

        <section className="border-b border-border">
          <div className="mx-auto max-w-[1100px] px-6 py-12">
            <p className="text-sm text-muted-foreground">Pick a store. Your jobs, handlers and registration stay the same.</p>
            <ul className="mt-6 grid gap-3 sm:grid-cols-3">
              {stores.map(({ name, package: pkg, Icon, fit }) => (
                <li key={name}>
                  <Link href={`/docs/${name.toLowerCase()}`} className="group flex h-full flex-col gap-3 rounded-xl border border-border bg-card p-5 transition-[border-color,box-shadow] duration-200 hover:border-border-strong hover:shadow-lift">
                    <div className="flex items-center gap-3">
                      <span className="grid size-9 place-items-center rounded-lg bg-primary-soft text-primary">
                        <Icon className="size-[18px]" />
                      </span>
                      <div>
                        <p className="font-medium">{name}</p>
                        <p className="font-mono text-xs text-muted-foreground">{pkg}</p>
                      </div>
                    </div>
                    <p className="text-sm leading-6 text-muted-foreground">{fit}</p>
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        </section>

        <section className="mx-auto grid max-w-[1100px] gap-12 px-6 py-16 lg:grid-cols-[1fr_1fr] lg:items-start lg:py-20">
          <div>
            <h2 className="max-w-xl text-2xl font-semibold tracking-tight sm:text-3xl">Built for jobs that matter</h2>
            <p className="mt-3 max-w-xl leading-7 text-muted-foreground">
              A lost welcome email is an annoyance. A lost invoice is not. Runwick is careful about the gap between the two, and tells you exactly where its guarantees stop.
            </p>
            <div className="mt-10 grid gap-y-7">
              {reasons.map(({ title, body }) => (
                <div key={title}>
                  <h3 className="font-semibold">{title}</h3>
                  <p className="mt-1.5 max-w-xl text-sm leading-6 text-muted-foreground">{body}</p>
                </div>
              ))}
            </div>
            <Link href="/docs/delivery-guarantees" className="mt-8 inline-flex items-center gap-1.5 text-sm font-medium text-primary hover:underline">
              What Runwick promises, and what it does not
              <ArrowRight className="size-3.5" />
            </Link>
          </div>
          <div className="lg:sticky lg:top-24">
            <JobHistory />
          </div>
        </section>

        <section className="border-t border-border">
          <div className="mx-auto max-w-[1100px] px-6 py-16 lg:py-20">
          <div className="grid gap-10 lg:grid-cols-[0.8fr_1.2fr]">
            <div>
              <h2 className="text-2xl font-semibold tracking-tight sm:text-3xl">See every job</h2>
              <p className="mt-3 leading-7 text-muted-foreground">
                The dashboard ships inside the <code className="font-mono text-[0.9em]">Runwick.Dashboard</code> package, with no Node at runtime. It shows each job's status history, attempts and the resources they used, and lets operators cancel, retry and pause queues with the same permissions as the API.
              </p>
              <Link href="/docs/dashboard" className="mt-5 inline-flex items-center gap-1.5 text-sm font-medium text-primary hover:underline">
                About the dashboard
                <ArrowRight className="size-3.5" />
              </Link>
            </div>
            <ul className="grid content-start gap-x-8 sm:grid-cols-2">
              {included.map(([label, href]) => (
                <li key={label} className="border-b border-border">
                  <Link href={href} className="flex items-center justify-between gap-3 py-3 text-sm transition-colors hover:text-primary">
                    {label}
                    <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />
                  </Link>
                </li>
              ))}
            </ul>
          </div>
          <img
            src="/images/dashboard-job.png"
            alt="The Runwick dashboard showing a job's details, status history and the resources each attempt used"
            loading="lazy"
            className="mt-12 w-full rounded-xl border border-border shadow-float"
          />
          </div>
        </section>

        <section className="border-y border-border bg-muted/40">
          <div className="mx-auto max-w-[1100px] px-6 py-16 lg:py-20">
            <h2 className="text-2xl font-semibold tracking-tight sm:text-3xl">How it works</h2>
            <ol className="mt-10 grid gap-8 sm:grid-cols-3">
              {steps.map(({ title, body }, index) => (
                <li key={title}>
                  <span className="font-mono text-sm text-muted-foreground">{index + 1}</span>
                  <h3 className="mt-2 font-semibold">{title}</h3>
                  <p className="mt-1.5 text-sm leading-6 text-muted-foreground">{body}</p>
                </li>
              ))}
            </ol>
          </div>
        </section>

        <section>
          <div className="mx-auto max-w-[1100px] px-6 py-16 lg:py-20">
            <h2 className="text-2xl font-semibold tracking-tight sm:text-3xl">Working with an AI agent?</h2>
            <p className="mt-3 max-w-2xl leading-7 text-muted-foreground">
              The whole documentation is available as plain text, and the <code className="font-mono text-[0.9em]">runwick</code> CLI has an agent mode with stable, machine-readable output.
            </p>
            <div className="mt-6 flex flex-wrap gap-3 text-sm">
              <Link href="/docs/ai-agents" className="inline-flex h-10 items-center gap-2 rounded-md border border-border px-4 font-medium transition-colors hover:bg-accent">
                Read the agent guide
                <ArrowRight className="size-4" />
              </Link>
              <a href="/llms.txt" className="inline-flex h-10 items-center rounded-md px-3 font-mono text-primary underline decoration-primary/35 underline-offset-4 transition-colors hover:decoration-primary">
                /llms.txt
              </a>
              <a href="/llms-full.txt" className="inline-flex h-10 items-center rounded-md px-3 font-mono text-primary underline decoration-primary/35 underline-offset-4 transition-colors hover:decoration-primary">
                /llms-full.txt
              </a>
            </div>
          </div>
        </section>
      </main>

      <Footer />
    </div>
  )
}

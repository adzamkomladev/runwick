import { Link } from '@inertiajs/react'
import { ArrowLeft, ArrowRight, Pencil } from 'lucide-react'
import { getAdjacent, type DocPage } from '../lib/content'
import { cn } from '../lib/utils'

const card =
  'group flex flex-col gap-1 rounded-xl border border-border bg-card px-5 py-4 transition-[border-color,box-shadow,transform] duration-200 ease-[var(--ease-out)] hover:-translate-y-px hover:border-primary/40 hover:shadow-lift'

export function PageFooter({ page }: { page: DocPage }) {
  const { previous, next } = getAdjacent(page.slug)

  return (
    <footer className="mt-16">
      <nav className="grid gap-3 sm:grid-cols-2" aria-label="Pagination">
        {previous ? (
          <Link href={previous.url} className={card}>
            <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <ArrowLeft className="size-3.5 transition-transform duration-200 ease-[var(--ease-out)] group-hover:-translate-x-1" />
              Previous
            </span>
            <span className="font-semibold text-foreground transition-colors group-hover:text-primary">{previous.title}</span>
            <span className="text-xs text-muted-foreground">{previous.section}</span>
          </Link>
        ) : (
          <span className="hidden sm:block" />
        )}
        {next ? (
          <Link href={next.url} className={cn(card, 'text-right sm:col-start-2')}>
            <span className="flex items-center justify-end gap-1.5 text-xs text-muted-foreground">
              Next
              <ArrowRight className="size-3.5 transition-transform duration-200 ease-[var(--ease-out)] group-hover:translate-x-1" />
            </span>
            <span className="font-semibold text-foreground transition-colors group-hover:text-primary">{next.title}</span>
            <span className="text-xs text-muted-foreground">{next.section}</span>
          </Link>
        ) : null}
      </nav>

      <div className="mt-10 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-6 text-[13px] text-muted-foreground">
        <a
          href={`https://github.com/adzamkomladev/runwick/edit/main/docs/Runwick.Docs/content/docs/${page.slug}.mdx`}
          target="_blank"
          rel="noreferrer"
          className="group inline-flex items-center gap-1.5 transition-colors hover:text-foreground"
        >
          <Pencil className="size-3.5 transition-transform duration-200 group-hover:-rotate-12" />
          Edit this page on GitHub
        </a>
        <span>Runwick is MIT licensed.</span>
      </div>
    </footer>
  )
}

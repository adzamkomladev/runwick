import { Link } from '@inertiajs/react'
import { ChevronRight } from 'lucide-react'
import { useMemo } from 'react'
import { DocsLayout } from '../../components/DocsLayout'
import { mdxComponents } from '../../components/Mdx'
import { PageFooter } from '../../components/PageFooter'
import { Seo } from '../../components/Seo'
import { InlineContents } from '../../components/TableOfContents'
import { getHeadings, getPage } from '../../lib/content'
import { getDocModule } from '../../lib/page-content'

export default function Show({ slug }: { slug: string }) {
  const page = getPage(slug)
  const module = getDocModule(slug)
  const headings = useMemo(() => getHeadings(slug), [slug])

  if (!page || !module) {
    return (
      <div className="mx-auto max-w-xl px-6 py-24 text-center">
        <p className="text-sm font-medium text-primary">404</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Page not found</h1>
        <p className="mt-3 text-muted-foreground">The documentation page “{slug}” does not exist.</p>
        <Link href="/docs/introduction" className="mt-6 inline-flex text-sm font-medium text-primary hover:underline">
          Back to the documentation
        </Link>
      </div>
    )
  }

  const Content = module.default

  return (
    <>
      <Seo
        title={`${page.title} — Runwick Docs`}
        description={page.description ?? `Runwick documentation: ${page.title}`}
        type="article"
        section={page.section}
      />
      <DocsLayout page={page} headings={headings}>
        <nav className="mb-5 flex items-center gap-1.5 text-[13px] text-muted-foreground" aria-label="Breadcrumb">
          <Link href="/docs/introduction" className="transition-colors hover:text-foreground">
            Docs
          </Link>
          <ChevronRight className="size-3.5 text-muted-foreground/60" />
          <span className="font-medium text-primary">{page.section}</span>
        </nav>

        <header className="mb-10">
          <h1 className="text-balance text-[2rem] font-semibold leading-[1.15] tracking-[-0.03em] sm:text-[2.5rem]">{page.title}</h1>
          {page.description && <p className="mt-4 max-w-[60ch] text-pretty text-[17px] leading-7 text-muted-foreground">{page.description}</p>}
        </header>

        <InlineContents headings={headings} />

        <div className="prose prose-neutral max-w-none dark:prose-invert prose-li:my-1.5">
          <Content components={mdxComponents} />
        </div>

        <PageFooter page={page} />
      </DocsLayout>
    </>
  )
}

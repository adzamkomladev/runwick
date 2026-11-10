import { ChevronDown } from 'lucide-react'
import type { ReactNode } from 'react'

export function Accordion({ children }: { children: ReactNode }) {
  return <div className="not-prose my-7 divide-y divide-border border-y border-border">{children}</div>
}

export function AccordionItem({ title, children }: { title: string; children: ReactNode }) {
  return (
    <details className="group [&_summary::-webkit-details-marker]:hidden">
      <summary className="flex cursor-pointer list-none items-center justify-between gap-4 py-3 text-sm font-medium transition-colors duration-150 hover:text-primary">
        {title}
        <ChevronDown className="size-4 shrink-0 text-muted-foreground transition-transform duration-200 ease-[var(--ease-out)] group-open:rotate-180" />
      </summary>
      <div className="pb-4 pt-1 text-sm leading-6 text-muted-foreground [&>p]:m-0 [&>p+p]:mt-3 [&_:not(pre)>code]:rounded-md [&_:not(pre)>code]:border [&_:not(pre)>code]:border-border [&_:not(pre)>code]:bg-muted [&_:not(pre)>code]:px-1 [&_:not(pre)>code]:py-0.5 [&_:not(pre)>code]:font-mono [&_:not(pre)>code]:text-[0.85em]">
        {children}
      </div>
    </details>
  )
}

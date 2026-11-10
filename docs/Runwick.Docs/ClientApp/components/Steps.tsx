import type { ReactNode } from 'react'

export function Steps({ children }: { children: ReactNode }) {
  return (
    <div className="not-prose my-8 [counter-reset:step] [&>ol]:m-0 [&>ol]:list-none [&>ol]:p-0">
      <ol>{children}</ol>
    </div>
  )
}

// Numbered markers sit on a hairline that runs to the next step.
export function Step({ title, children }: { title?: string; children: ReactNode }) {
  return (
    <li className="relative pb-7 pl-11 [counter-increment:step] last:pb-0 before:absolute before:left-0 before:top-0 before:z-10 before:grid before:size-7 before:place-items-center before:rounded-full before:border before:border-border-strong before:bg-background before:text-xs before:font-semibold before:tabular-nums before:text-foreground before:content-[counter(step)] after:absolute after:bottom-1 after:left-[13.5px] after:top-9 after:w-px after:bg-border last:after:hidden">
      {title && <p className="mb-1.5 pt-0.5 font-semibold leading-6 text-foreground">{title}</p>}
      <div className="text-[15px] leading-7 text-muted-foreground [&>p]:m-0 [&>p+p]:mt-3 [&_a]:font-medium [&_a]:text-primary [&_a]:underline [&_a]:decoration-primary/35 [&_a]:underline-offset-4 [&_:not(pre)>code]:rounded-md [&_:not(pre)>code]:border [&_:not(pre)>code]:border-border [&_:not(pre)>code]:bg-muted [&_:not(pre)>code]:px-1 [&_:not(pre)>code]:py-0.5 [&_:not(pre)>code]:font-mono [&_:not(pre)>code]:text-[0.85em] [&_:not(pre)>code]:text-foreground [&_figure]:my-4">
        {children}
      </div>
    </li>
  )
}

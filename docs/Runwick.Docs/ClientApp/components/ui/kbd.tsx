import type { ComponentProps } from 'react'
import { cn } from '../../lib/utils'

export function Kbd({ className, ...props }: ComponentProps<'kbd'>) {
  return (
    <kbd
      className={cn(
        'pointer-events-none inline-flex h-5 min-w-5 select-none items-center justify-center rounded-md border border-border border-b-border-strong bg-background px-1.5 font-sans text-[10px] font-medium text-muted-foreground shadow-[0_1px_0_var(--border)]',
        className,
      )}
      {...props}
    />
  )
}

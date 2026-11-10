import { Link } from '@inertiajs/react'
import { Zap } from 'lucide-react'
import { cn } from '../lib/utils'

// Same mark as the dashboard: a lime bolt on midnight violet, inverted in dark mode.
export function Brand({ className, compact = false }: { className?: string; compact?: boolean }) {
  return (
    <Link href="/" className={cn('group flex items-center gap-2.5 rounded-md font-semibold tracking-tight', className)}>
      <span className="grid size-7 place-items-center rounded-lg bg-foreground text-highlight ring-1 ring-foreground/10 transition-transform duration-300 ease-[var(--ease-out)] group-hover:-rotate-12 dark:bg-highlight dark:text-highlight-foreground dark:ring-0">
        <Zap className="size-4" strokeWidth={2.2} fill="currentColor" />
      </span>
      {!compact && <span className="text-[15px]">Runwick</span>}
    </Link>
  )
}

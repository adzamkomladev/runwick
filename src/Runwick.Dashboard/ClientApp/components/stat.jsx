import { AnimatedNumber } from '../lib'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card'
import { cn } from './ui/utils'

/** A stat on a detail page: a label, a value that tweens when a number, and a line under it. */
export function StatCard({ label, value, more, suffix, foot, icon: Icon, testId, children, className }) {
  return (
    <Card className={cn('gap-2', className)}>
      <CardHeader>
        <CardDescription className="flex items-center gap-1.5">{Icon && <Icon className="size-4" aria-hidden />}{label}</CardDescription>
        <CardTitle data-testid={testId} className="flex min-w-0 items-baseline gap-1 text-2xl font-semibold tracking-tight">
          {typeof value === 'number' ? <AnimatedNumber value={value} more={more} /> : <span className="min-w-0 truncate">{value ?? '—'}</span>}
          {suffix && <span className="text-base text-muted-foreground">{suffix}</span>}
        </CardTitle>
      </CardHeader>
      {(foot || children) && <CardContent className="grid gap-2 text-xs text-muted-foreground">{children}{foot && <span>{foot}</span>}</CardContent>}
    </Card>
  )
}

/** A label and its value in a definition list. */
export const Field = ({ label, children, className }) => (
  <div className={cn('grid min-w-0 gap-1', className)}><dt className="text-xs text-muted-foreground">{label}</dt><dd className="min-w-0 text-sm break-words">{children ?? '—'}</dd></div>
)

/** Milliseconds as a short duration ("60 s", "1.5 min", "2 h"). */
export function ms(v) {
  if (v == null) return '—'
  if (v < 1000) return `${v} ms`
  const s = v / 1000
  return s < 120 ? `${+s.toFixed(1)} s` : s < 7200 ? `${+(s / 60).toFixed(1)} min` : `${+(s / 3600).toFixed(1)} h`
}

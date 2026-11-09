import { cn } from './utils'

export const Card = ({ className, ...props }) =>
  <div data-slot="card" className={cn('flex flex-col gap-4 rounded-xl border bg-card py-5 text-card-foreground shadow-xs', className)} {...props} />
export const CardHeader = ({ className, ...props }) =>
  <div data-slot="card-header" className={cn('grid auto-rows-min items-start gap-1 px-5 has-data-[slot=card-action]:grid-cols-[1fr_auto]', className)} {...props} />
export const CardTitle = ({ className, ...props }) => <div data-slot="card-title" className={cn('font-semibold leading-none', className)} {...props} />
export const CardDescription = ({ className, ...props }) => <div data-slot="card-description" className={cn('text-sm text-muted-foreground', className)} {...props} />
export const CardAction = ({ className, ...props }) =>
  <div data-slot="card-action" className={cn('col-start-2 row-span-2 row-start-1 self-start justify-self-end', className)} {...props} />
export const CardContent = ({ className, ...props }) => <div data-slot="card-content" className={cn('px-5', className)} {...props} />
export const CardFooter = ({ className, ...props }) => <div data-slot="card-footer" className={cn('flex items-center px-5', className)} {...props} />

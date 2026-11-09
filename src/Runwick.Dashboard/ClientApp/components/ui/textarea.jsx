import { cn } from './utils'

export const Textarea = ({ className, ...props }) => (
  <textarea data-slot="textarea" className={cn(
    'flex min-h-16 w-full resize-none rounded-md border border-input bg-card px-3 py-2 text-sm shadow-xs transition-[color,box-shadow] outline-none placeholder:text-muted-foreground/70 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50',
    className)} {...props} />
)

import { cn } from './utils'

export const Input = ({ className, type, ...props }) => (
  <input type={type} data-slot="input" className={cn(
    'flex h-9 w-full min-w-0 rounded-md border border-input bg-card px-3 py-1 text-sm shadow-xs transition-[color,box-shadow] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50',
    className)} {...props} />
)

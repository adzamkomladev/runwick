import { cn } from './utils'

export const Skeleton = ({ className, ...props }) => <div data-slot="skeleton" className={cn('animate-pulse rounded-md bg-muted', className)} {...props} />

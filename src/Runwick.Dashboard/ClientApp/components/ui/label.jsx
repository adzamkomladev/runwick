import { Label as LabelPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Label = ({ className, ...props }) =>
  <LabelPrimitive.Root data-slot="label" className={cn('flex select-none items-center gap-2 text-sm font-medium leading-none', className)} {...props} />

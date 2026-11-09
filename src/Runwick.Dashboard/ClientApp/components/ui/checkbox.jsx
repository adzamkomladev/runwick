import { IconCheck } from '@tabler/icons-react'
import { Checkbox as CheckboxPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Checkbox = ({ className, ...props }) => (
  <CheckboxPrimitive.Root data-slot="checkbox" className={cn(
    'peer size-4 shrink-0 rounded-[4px] border border-input bg-card shadow-xs transition-[background-color,box-shadow,transform] outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50 active:scale-90 disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:border-primary data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground',
    className)} {...props}>
    <CheckboxPrimitive.Indicator className="flex items-center justify-center text-current data-[state=checked]:animate-in data-[state=checked]:zoom-in-50">
      <IconCheck className="size-3.5" stroke={3} />
    </CheckboxPrimitive.Indicator>
  </CheckboxPrimitive.Root>
)

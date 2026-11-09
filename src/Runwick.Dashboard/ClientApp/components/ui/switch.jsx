import { Switch as SwitchPrimitive } from 'radix-ui'
import { cn } from './utils'

/** A Radix switch (role="switch", Space toggles); the thumb slides, cut by reduced motion. */
export const Switch = ({ className, ...props }) => (
  <SwitchPrimitive.Root data-slot="switch" className={cn(
    'peer inline-flex h-5 w-9 shrink-0 items-center rounded-full border border-transparent shadow-xs press outline-none focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-primary data-[state=unchecked]:bg-input',
    className)} {...props}>
    <SwitchPrimitive.Thumb className="pointer-events-none block size-4 rounded-full bg-card shadow-sm ring-0 transition-transform data-[state=checked]:translate-x-4 data-[state=unchecked]:translate-x-0.5" />
  </SwitchPrimitive.Root>
)

import { Popover as PopoverPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Popover = (props) => <PopoverPrimitive.Root data-slot="popover" {...props} />
export const PopoverTrigger = (props) => <PopoverPrimitive.Trigger data-slot="popover-trigger" {...props} />
export const PopoverAnchor = (props) => <PopoverPrimitive.Anchor {...props} />
export const PopoverClose = (props) => <PopoverPrimitive.Close {...props} />

export const PopoverContent = ({ className, align = 'start', sideOffset = 6, ...props }) => (
  <PopoverPrimitive.Portal>
    <PopoverPrimitive.Content data-slot="popover-content" align={align} sideOffset={sideOffset} className={cn(
      'z-50 max-h-(--radix-popover-content-available-height) overflow-auto rounded-lg border bg-popover p-3 text-popover-foreground shadow-lg outline-none data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95',
      className)} {...props} />
  </PopoverPrimitive.Portal>
)

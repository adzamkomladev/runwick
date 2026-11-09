import { Tooltip as TooltipPrimitive } from 'radix-ui'
import { cn } from './utils'

export const TooltipProvider = ({ delayDuration = 150, ...props }) => <TooltipPrimitive.Provider delayDuration={delayDuration} {...props} />
export const Tooltip = (props) => <TooltipPrimitive.Root data-slot="tooltip" {...props} />
export const TooltipTrigger = (props) => <TooltipPrimitive.Trigger data-slot="tooltip-trigger" {...props} />
export const TooltipContent = ({ className, sideOffset = 4, children, ...props }) => (
  <TooltipPrimitive.Portal>
    <TooltipPrimitive.Content data-slot="tooltip-content" sideOffset={sideOffset} className={cn(
      'z-50 w-fit max-w-xs rounded-md bg-foreground px-3 py-1.5 text-xs text-background animate-in fade-in-0 zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95',
      className)} {...props}>
      {children}
    </TooltipPrimitive.Content>
  </TooltipPrimitive.Portal>
)

import { IconCheck } from '@tabler/icons-react'
import { DropdownMenu as DropdownMenuPrimitive } from 'radix-ui'
import { cn } from './utils'

export const DropdownMenu = (props) => <DropdownMenuPrimitive.Root data-slot="dropdown-menu" {...props} />
export const DropdownMenuTrigger = (props) => <DropdownMenuPrimitive.Trigger data-slot="dropdown-menu-trigger" {...props} />
export const DropdownMenuContent = ({ className, sideOffset = 4, ...props }) => (
  <DropdownMenuPrimitive.Portal>
    <DropdownMenuPrimitive.Content data-slot="dropdown-menu-content" sideOffset={sideOffset} className={cn(
      'z-50 min-w-[8rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95',
      className)} {...props} />
  </DropdownMenuPrimitive.Portal>
)
export const DropdownMenuLabel = ({ className, ...props }) =>
  <DropdownMenuPrimitive.Label className={cn('px-2 py-1.5 text-xs font-medium text-muted-foreground', className)} {...props} />
export const DropdownMenuSeparator = ({ className, ...props }) => <DropdownMenuPrimitive.Separator className={cn('-mx-1 my-1 h-px bg-border', className)} {...props} />
export const DropdownMenuRadioGroup = (props) => <DropdownMenuPrimitive.RadioGroup {...props} />
export const DropdownMenuRadioItem = ({ className, children, ...props }) => (
  <DropdownMenuPrimitive.RadioItem className={cn(
    'relative flex cursor-default select-none items-center gap-2 rounded-sm py-1.5 pr-2 pl-8 text-sm outline-none focus:bg-accent focus:text-accent-foreground [&_svg]:size-4',
    className)} {...props}>
    <span className="pointer-events-none absolute left-2 flex size-3.5 items-center justify-center">
      <DropdownMenuPrimitive.ItemIndicator><IconCheck className="size-4" /></DropdownMenuPrimitive.ItemIndicator>
    </span>
    {children}
  </DropdownMenuPrimitive.RadioItem>
)
export const DropdownMenuItem = ({ className, ...props }) => (
  <DropdownMenuPrimitive.Item className={cn(
    'relative flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 [&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-muted-foreground',
    className)} {...props} />
)
/** A radio item drawn as one segment of a compact icon toggle (the theme switch in the account menu). */
export const DropdownMenuSegment = ({ className, ...props }) => (
  <DropdownMenuPrimitive.RadioItem className={cn(
    'grid size-7 cursor-default place-items-center rounded-full text-muted-foreground outline-none transition-colors hover:text-foreground focus:ring-2 focus:ring-ring/50 data-[state=checked]:bg-background data-[state=checked]:text-foreground data-[state=checked]:shadow-sm data-[state=checked]:ring-1 data-[state=checked]:ring-border [&_svg]:size-4',
    className)} {...props} />
)

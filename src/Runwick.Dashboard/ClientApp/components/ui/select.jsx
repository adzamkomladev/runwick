import { IconCheck, IconChevronDown } from '@tabler/icons-react'
import { Select as SelectPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Select = (props) => <SelectPrimitive.Root data-slot="select" {...props} />
export const SelectValue = (props) => <SelectPrimitive.Value data-slot="select-value" {...props} />

export const SelectTrigger = ({ className, children, ...props }) => (
  <SelectPrimitive.Trigger data-slot="select-trigger" className={cn(
    'group/select flex h-9 w-fit items-center justify-between gap-2 whitespace-nowrap rounded-md border border-input bg-card px-3 py-2 text-sm shadow-xs transition-[color,box-shadow] outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-[placeholder]:text-muted-foreground [&_svg]:size-4 [&_svg]:shrink-0',
    className)} {...props}>
    {children}
    <SelectPrimitive.Icon asChild><IconChevronDown className="opacity-50 transition-transform duration-200 group-data-[state=open]/select:rotate-180" /></SelectPrimitive.Icon>
  </SelectPrimitive.Trigger>
)

export const SelectContent = ({ className, children, position = 'popper', ...props }) => (
  <SelectPrimitive.Portal>
    <SelectPrimitive.Content data-slot="select-content" position={position} className={cn(
      'fade-y relative z-50 max-h-(--radix-select-content-available-height) min-w-[8rem] overflow-y-auto rounded-md border bg-popover text-popover-foreground shadow-md data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0',
      position === 'popper' && 'data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1', className)} {...props}>
      <SelectPrimitive.Viewport className={cn('p-1', position === 'popper' && 'w-full min-w-(--radix-select-trigger-width)')}>{children}</SelectPrimitive.Viewport>
    </SelectPrimitive.Content>
  </SelectPrimitive.Portal>
)

export const SelectItem = ({ className, children, ...props }) => (
  <SelectPrimitive.Item data-slot="select-item" className={cn(
    'relative flex w-full cursor-default select-none items-center gap-2 rounded-sm py-1.5 pr-8 pl-2 text-sm press outline-none focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50',
    className)} {...props}>
    <span className="absolute right-2 flex size-3.5 items-center justify-center">
      <SelectPrimitive.ItemIndicator><IconCheck className="size-4" /></SelectPrimitive.ItemIndicator>
    </span>
    <SelectPrimitive.ItemText>{children}</SelectPrimitive.ItemText>
  </SelectPrimitive.Item>
)

import { IconX } from '@tabler/icons-react'
import { Dialog as SheetPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Sheet = (props) => <SheetPrimitive.Root data-slot="sheet" {...props} />
export const SheetTrigger = (props) => <SheetPrimitive.Trigger data-slot="sheet-trigger" {...props} />
export const SheetClose = (props) => <SheetPrimitive.Close data-slot="sheet-close" {...props} />

const sides = {
  right: 'inset-y-0 right-0 h-full w-full border-l data-[state=open]:slide-in-from-right data-[state=closed]:slide-out-to-right sm:max-w-xl',
  left: 'inset-y-0 left-0 h-full w-72 border-r data-[state=open]:slide-in-from-left data-[state=closed]:slide-out-to-left',
}

export const SheetContent = ({ className, children, side = 'right', ...props }) => (
  <SheetPrimitive.Portal>
    <SheetPrimitive.Overlay className="fixed inset-0 z-50 bg-black/50 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=closed]:animate-out data-[state=closed]:fade-out-0" />
    <SheetPrimitive.Content data-slot="sheet-content" className={cn(
      'fixed z-50 flex flex-col gap-4 bg-popover text-popover-foreground shadow-lg transition ease-in-out data-[state=open]:animate-in data-[state=open]:duration-300 data-[state=closed]:animate-out data-[state=closed]:duration-200',
      sides[side], className)} {...props}>
      {children}
      <SheetPrimitive.Close className="absolute top-4 right-4 rounded-sm opacity-70 transition-opacity outline-none hover:opacity-100 focus-visible:ring-[3px] focus-visible:ring-ring/50 [&_svg]:size-4">
        <IconX /><span className="sr-only">Close</span>
      </SheetPrimitive.Close>
    </SheetPrimitive.Content>
  </SheetPrimitive.Portal>
)
export const SheetHeader = ({ className, ...props }) => <div className={cn('flex flex-col gap-1.5 p-5 pr-12', className)} {...props} />
export const SheetTitle = ({ className, ...props }) => <SheetPrimitive.Title className={cn('font-semibold', className)} {...props} />
export const SheetDescription = ({ className, ...props }) => <SheetPrimitive.Description className={cn('text-sm text-muted-foreground', className)} {...props} />

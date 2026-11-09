import { IconX } from '@tabler/icons-react'
import { Dialog as DialogPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Dialog = (props) => <DialogPrimitive.Root data-slot="dialog" {...props} />
export const DialogTrigger = (props) => <DialogPrimitive.Trigger data-slot="dialog-trigger" {...props} />
export const DialogClose = (props) => <DialogPrimitive.Close data-slot="dialog-close" {...props} />

export const DialogOverlay = ({ className, ...props }) => (
  <DialogPrimitive.Overlay data-slot="dialog-overlay" className={cn(
    'fixed inset-0 z-50 bg-black/50 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=closed]:animate-out data-[state=closed]:fade-out-0', className)} {...props} />
)

export const DialogContent = ({ className, children, ...props }) => (
  <DialogPrimitive.Portal>
    <DialogOverlay />
    <DialogPrimitive.Content data-slot="dialog-content" className={cn(
      'fixed top-1/2 left-1/2 z-50 grid w-[calc(100%-2rem)] max-w-lg -translate-x-1/2 -translate-y-1/2 gap-4 rounded-xl border bg-popover p-6 text-popover-foreground shadow-lg duration-200 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95',
      className)} {...props}>
      {children}
      <DialogPrimitive.Close className="absolute top-4 right-4 rounded-sm opacity-70 press outline-none hover:opacity-100 focus-visible:ring-[3px] focus-visible:ring-ring/50 [&_svg]:size-4">
        <IconX /><span className="sr-only">Close</span>
      </DialogPrimitive.Close>
    </DialogPrimitive.Content>
  </DialogPrimitive.Portal>
)

export const DialogHeader = ({ className, ...props }) => <div data-slot="dialog-header" className={cn('flex flex-col gap-2 text-left', className)} {...props} />
export const DialogFooter = ({ className, ...props }) =>
  <div data-slot="dialog-footer" className={cn('flex flex-col-reverse gap-2 sm:flex-row sm:justify-end', className)} {...props} />
export const DialogTitle = ({ className, ...props }) => <DialogPrimitive.Title data-slot="dialog-title" className={cn('text-lg font-semibold leading-none', className)} {...props} />
export const DialogDescription = ({ className, ...props }) =>
  <DialogPrimitive.Description data-slot="dialog-description" className={cn('text-sm text-muted-foreground', className)} {...props} />

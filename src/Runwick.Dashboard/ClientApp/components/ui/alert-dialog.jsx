import { AlertDialog as AlertDialogPrimitive } from 'radix-ui'
import { buttonVariants } from './button'
import { cn } from './utils'

export const AlertDialog = (props) => <AlertDialogPrimitive.Root data-slot="alert-dialog" {...props} />

// No exit animation: a fast command can reopen the dialog within 200 ms, and reopening mid-exit left an overlay above it.
export const AlertDialogContent = ({ className, ...props }) => (
  <AlertDialogPrimitive.Portal>
    <AlertDialogPrimitive.Overlay className="fixed inset-0 z-50 bg-black/50 data-[state=open]:animate-in data-[state=open]:fade-in-0" />
    <AlertDialogPrimitive.Content data-slot="alert-dialog-content" className={cn(
      'fixed top-1/2 left-1/2 z-50 grid w-[calc(100%-2rem)] max-w-md -translate-x-1/2 -translate-y-1/2 gap-4 rounded-xl border bg-popover p-6 text-popover-foreground shadow-lg duration-200 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95',
      className)} {...props} />
  </AlertDialogPrimitive.Portal>
)
export const AlertDialogHeader = ({ className, ...props }) => <div className={cn('flex flex-col gap-2 text-left', className)} {...props} />
export const AlertDialogFooter = ({ className, ...props }) => <div className={cn('flex flex-col-reverse gap-2 sm:flex-row sm:justify-end', className)} {...props} />
export const AlertDialogTitle = ({ className, ...props }) => <AlertDialogPrimitive.Title className={cn('text-lg font-semibold', className)} {...props} />
export const AlertDialogDescription = ({ className, ...props }) =>
  <AlertDialogPrimitive.Description className={cn('text-sm text-muted-foreground', className)} {...props} />
export const AlertDialogAction = ({ className, variant, ...props }) =>
  <AlertDialogPrimitive.Action className={cn(buttonVariants({ variant }), className)} {...props} />
export const AlertDialogCancel = ({ className, ...props }) =>
  <AlertDialogPrimitive.Cancel className={cn(buttonVariants({ variant: 'outline' }), className)} {...props} />

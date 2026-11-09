import { IconAlertTriangle, IconCircleCheck, IconInfoCircle, IconX } from '@tabler/icons-react'
import { Toast as ToastPrimitive } from 'radix-ui'
import { useSyncExternalStore } from 'react'
import { cn } from './utils'

// Toasts for command results (shadcn/ui's Radix toast). A tiny module store, so a toast outlives the page that raised it.
// A toast with an id replaces the one with the same id: one command result is shown at a time.
let toasts = []
const listeners = new Set()
const emit = () => listeners.forEach((l) => l())
let next = 0

export function toast({ id = `t${++next}`, ...t }) {
  toasts = [...toasts.filter((x) => x.id !== id), { ...t, id, key: ++next, open: true }]
  emit()
  return id
}
export function dismissToast(id) {
  toasts = toasts.map((x) => (x.id === id ? { ...x, open: false } : x))
  emit()
}
const remove = (key) => { toasts = toasts.filter((x) => x.key !== key); emit() }

const icons = { success: IconCircleCheck, destructive: IconAlertTriangle, default: IconInfoCircle }
const tones = {
  success: 'border-success/40 [&_[data-icon]]:text-success',
  destructive: 'border-destructive/40 [&_[data-icon]]:text-destructive',
  default: '[&_[data-icon]]:text-primary',
}

export function Toaster() {
  const list = useSyncExternalStore((l) => { listeners.add(l); return () => listeners.delete(l) }, () => toasts)
  return (
    <ToastPrimitive.Provider swipeDirection="right" duration={6000}>
      {list.map(({ id, key, open, title, description, variant = 'default', testId }) => {
        const Icon = icons[variant]
        return (
          <ToastPrimitive.Root key={key} open={open} data-testid={testId}
            onOpenChange={(o) => { if (!o) { dismissToast(id); setTimeout(() => remove(key), 300) } }}
            className={cn(
              'group pointer-events-auto relative flex w-full items-start gap-3 overflow-hidden rounded-lg border bg-popover p-4 pr-9 text-popover-foreground shadow-lg transition-all data-[swipe=cancel]:translate-x-0 data-[swipe=end]:translate-x-(--radix-toast-swipe-end-x) data-[swipe=move]:translate-x-(--radix-toast-swipe-move-x) data-[swipe=move]:transition-none data-[state=open]:animate-in data-[state=open]:slide-in-from-bottom-4 data-[state=open]:fade-in-0 data-[state=closed]:animate-out data-[state=closed]:fade-out-80 data-[state=closed]:slide-out-to-right-full',
              tones[variant])}>
            <Icon data-icon className="mt-0.5 size-5 shrink-0" />
            <div className="grid min-w-0 gap-1">
              {title && <ToastPrimitive.Title className="text-sm font-semibold">{title}</ToastPrimitive.Title>}
              {description && <ToastPrimitive.Description className="text-sm break-words text-muted-foreground">{description}</ToastPrimitive.Description>}
            </div>
            <ToastPrimitive.Close aria-label="Dismiss" className="absolute top-2 right-2 rounded-md p-1 text-muted-foreground opacity-70 transition-opacity hover:opacity-100 focus-visible:opacity-100 focus-visible:ring-[3px] focus-visible:ring-ring/50 outline-none">
              <IconX className="size-4" />
            </ToastPrimitive.Close>
          </ToastPrimitive.Root>
        )
      })}
      <ToastPrimitive.Viewport className="fixed right-0 bottom-0 z-[100] flex max-h-screen w-full flex-col gap-2 p-4 outline-none sm:max-w-sm" />
    </ToastPrimitive.Provider>
  )
}

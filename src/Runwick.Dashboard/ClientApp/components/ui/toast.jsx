import { IconAlertTriangle, IconCircleCheck, IconCircleX, IconCopy, IconInfoCircle, IconLoader2, IconX } from '@tabler/icons-react'
import { Toast as ToastPrimitive } from 'radix-ui'
import { useSyncExternalStore } from 'react'
import { cn } from './utils'

// Sonner-style toasts on Radix Toast (Sonner itself injects a <style> element, which the dashboard CSP forbids). A tiny module
// store, so a toast outlives the page that raised it. A toast with an id replaces the one with that id; at most three show.
// Variants: success (applied), error (rejected or no answer), warning (outcome unknown), info, copied, loading.
let toasts = []
const listeners = new Set()
const emit = () => listeners.forEach((l) => l())
let next = 0

/** { id?, title, description?, variant?, testId?, action?: { label, onClick, testId }, duration? (ms; Infinity keeps it) } */
export function toast({ id = `t${++next}`, ...t }) {
  toasts = [...toasts.filter((x) => x.id !== id), { ...t, id, key: ++next, open: true }].slice(-3)
  emit()
  return id
}
export function dismissToast(id) {
  toasts = toasts.map((x) => (x.id === id ? { ...x, open: false } : x))
  emit()
}
const remove = (key) => { toasts = toasts.filter((x) => x.key !== key); emit() }

const look = {
  success: [IconCircleCheck, 'text-success'],
  error: [IconCircleX, 'text-destructive'],
  warning: [IconAlertTriangle, 'text-warning'],
  info: [IconInfoCircle, 'text-primary'],
  copied: [IconCopy, 'text-primary'],
  loading: [IconLoader2, 'animate-spin text-muted-foreground'],
}

export function Toaster() {
  const list = useSyncExternalStore((l) => { listeners.add(l); return () => listeners.delete(l) }, () => toasts)
  return (
    <ToastPrimitive.Provider swipeDirection="right" duration={4000} label="Notification">
      {list.map(({ id, key, open, title, description, variant = 'info', testId, action, duration }) => {
        const [Icon, tone] = look[variant] ?? look.info
        return (
          <ToastPrimitive.Root key={key} open={open} data-testid={testId} data-variant={variant} duration={duration ?? (variant === 'copied' ? 1800 : undefined)}
            type={variant === 'error' || variant === 'warning' ? 'foreground' : 'background'}
            onOpenChange={(o) => { if (!o) { dismissToast(id); setTimeout(() => remove(key), 250) } }}
            className={cn(
              'group pointer-events-auto relative flex w-full items-center gap-3 rounded-xl border bg-popover px-4 py-3 text-popover-foreground shadow-[0_6px_24px_-6px_rgb(0_0_0/0.18)] transition-all',
              'data-[swipe=cancel]:translate-x-0 data-[swipe=end]:translate-x-(--radix-toast-swipe-end-x) data-[swipe=move]:translate-x-(--radix-toast-swipe-move-x) data-[swipe=move]:transition-none',
              'data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:slide-in-from-bottom-3 data-[state=open]:zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95',
              variant === 'copied' && 'w-auto self-end py-2')}>
            <Icon aria-hidden className={cn('size-[1.15rem] shrink-0', tone)} />
            <div className="grid min-w-0 flex-1 gap-0.5">
              {title && <ToastPrimitive.Title className="text-[0.8125rem] leading-5 font-medium">{title}</ToastPrimitive.Title>}
              {description && <ToastPrimitive.Description className="text-xs leading-5 break-words text-muted-foreground">{description}</ToastPrimitive.Description>}
            </div>
            {action && <ToastPrimitive.Action altText={action.label} asChild>
              <button type="button" data-testid={action.testId} onClick={action.onClick}
                className="h-7 shrink-0 rounded-md bg-primary px-2.5 text-xs font-medium text-primary-foreground press outline-none hover:bg-primary/90 focus-visible:ring-[3px] focus-visible:ring-ring/50">{action.label}</button>
            </ToastPrimitive.Action>}
            {variant !== 'copied' && <ToastPrimitive.Close aria-label="Dismiss"
              className="absolute -top-1.5 -left-1.5 grid size-5 place-items-center rounded-full border bg-popover text-muted-foreground opacity-0 shadow-sm transition-opacity outline-none group-hover:opacity-100 focus-visible:opacity-100 focus-visible:ring-[3px] focus-visible:ring-ring/50">
              <IconX className="size-3" />
            </ToastPrimitive.Close>}
          </ToastPrimitive.Root>
        )
      })}
      <ToastPrimitive.Viewport className="fixed right-0 bottom-0 z-[100] flex max-h-screen w-full flex-col gap-2 p-4 outline-none sm:max-w-[22rem]" />
    </ToastPrimitive.Provider>
  )
}

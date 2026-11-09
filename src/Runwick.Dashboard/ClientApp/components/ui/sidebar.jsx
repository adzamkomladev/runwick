import { IconLayoutSidebar } from '@tabler/icons-react'
import { Slot } from 'radix-ui'
import { createContext, use, useEffect, useState, useSyncExternalStore } from 'react'
import { Button } from './button'
import { Sheet, SheetContent, SheetDescription, SheetTitle } from './sheet'
import { Tooltip, TooltipContent, TooltipTrigger } from './tooltip'
import { cn } from './utils'

// shadcn/ui-style sidebar: a column that collapses to an icon rail (Ctrl/Cmd+B), remembered per browser, and a sheet below md.

const KEY = 'runwick-dashboard-sidebar'
const mobileQuery = window.matchMedia('(max-width: 767px)')
const useMobile = () => useSyncExternalStore((l) => { mobileQuery.addEventListener('change', l); return () => mobileQuery.removeEventListener('change', l) }, () => mobileQuery.matches)
const readOpen = () => { try { return localStorage.getItem(KEY) !== 'false' } catch { return true } }

const SidebarContext = createContext(null)
export const useSidebar = () => use(SidebarContext)

export function SidebarProvider({ children }) {
  const mobile = useMobile()
  const [open, setOpenState] = useState(readOpen)
  const [openMobile, setOpenMobile] = useState(false)
  const setOpen = (next) => {
    setOpenState(next)
    try { localStorage.setItem(KEY, String(next)) } catch { /* blocked storage: the choice is not remembered */ }
  }
  const toggle = () => (mobile ? setOpenMobile((o) => !o) : setOpen(!open))
  useEffect(() => {
    const onKey = (e) => {
      if (e.key.toLowerCase() === 'b' && (e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey) {
        e.preventDefault()
        toggle()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  })
  return (
    <SidebarContext value={{ open, mobile, openMobile, setOpenMobile, toggle }}>
      <div data-sidebar={open ? 'expanded' : 'collapsed'} className="group/sidebar flex min-h-svh w-full">{children}</div>
    </SidebarContext>
  )
}

export function Sidebar({ label, children }) {
  const s = useSidebar()
  if (s.mobile) {
    return (
      <Sheet open={s.openMobile} onOpenChange={s.setOpenMobile}>
        <SheetContent side="left" className="w-72 gap-0 bg-sidebar p-0">
          <SheetTitle className="sr-only">{label}</SheetTitle>
          <SheetDescription className="sr-only">Dashboard pages</SheetDescription>
          <div className="flex min-h-0 flex-1 flex-col">{children}</div>
        </SheetContent>
      </Sheet>
    )
  }
  return (
    <aside aria-label={label} className="sticky top-0 flex h-svh w-60 shrink-0 flex-col border-r border-sidebar-border bg-sidebar transition-[width] duration-200 ease-out group-data-[sidebar=collapsed]/sidebar:w-14">
      {children}
    </aside>
  )
}

export const SidebarHeader = ({ className, ...props }) => <div className={cn('flex flex-col gap-2 p-2.5', className)} {...props} />
export const SidebarFooter = ({ className, ...props }) => <div className={cn('mt-auto flex flex-col gap-2 border-t border-sidebar-border p-2.5', className)} {...props} />
export const SidebarContent = ({ className, ...props }) =>
  <div className={cn('flex min-h-0 flex-1 flex-col gap-4 overflow-x-hidden overflow-y-auto p-2.5', className)} {...props} />

export function SidebarGroup({ label, children }) {
  return (
    <div className="grid gap-0.5">
      {label && <p className="flex h-7 items-center px-2 text-[0.7rem] font-medium tracking-wider text-muted-foreground uppercase transition-opacity duration-200 group-data-[sidebar=collapsed]/sidebar:h-0 group-data-[sidebar=collapsed]/sidebar:opacity-0">{label}</p>}
      <ul className="grid gap-0.5">{children}</ul>
    </div>
  )
}

const itemClass = cn(
  'group/item relative flex h-9 w-full min-w-0 items-center gap-2.5 overflow-hidden rounded-md px-2.5 text-sm text-muted-foreground outline-none transition-[color,background-color] duration-150',
  'hover:bg-accent hover:text-accent-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50',
  'data-[active=true]:bg-accent data-[active=true]:font-medium data-[active=true]:text-foreground',
  'before:absolute before:inset-y-2 before:left-0 before:w-0.5 before:scale-y-0 before:rounded-full before:bg-primary before:transition-transform before:duration-200 data-[active=true]:before:scale-y-100',
  '[&>svg]:size-4 [&>svg]:shrink-0 [&>svg]:transition-transform [&>svg]:duration-200 hover:[&>svg]:scale-110',
  '[&>span]:truncate [&>span]:transition-opacity [&>span]:duration-200 group-data-[sidebar=collapsed]/sidebar:[&>span]:sr-only',
)

/** One row. In the icon rail the label is visually hidden (still read out) and a tooltip names it. */
export function SidebarMenuButton({ asChild, active, tooltip, className, onClick, ...props }) {
  const s = useSidebar()
  const Comp = asChild ? Slot.Root : 'button'
  return (
    <li>
      <Tooltip>
        <TooltipTrigger asChild>
          <Comp data-active={active || undefined} aria-current={active ? 'page' : undefined} className={cn(itemClass, className)}
            onClick={(e) => { onClick?.(e); if (s.mobile) s.setOpenMobile(false) }} {...props} />
        </TooltipTrigger>
        <TooltipContent side="right" hidden={s.open || s.mobile}>{tooltip}</TooltipContent>
      </Tooltip>
    </li>
  )
}

export function SidebarTrigger({ className }) {
  const s = useSidebar()
  const label = s.mobile ? 'Open navigation' : s.open ? 'Collapse sidebar' : 'Expand sidebar'
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button variant="ghost" size="icon-sm" aria-label={label} aria-expanded={s.mobile ? s.openMobile : s.open}
          aria-keyshortcuts="Control+B Meta+B" className={className} onClick={s.toggle}>
          <IconLayoutSidebar className="transition-transform duration-200 group-data-[sidebar=collapsed]/sidebar:rotate-180" />
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label} <kbd className="ml-1.5 rounded border border-background/30 px-1 font-mono text-[0.65rem] opacity-80">Ctrl B</kbd></TooltipContent>
    </Tooltip>
  )
}

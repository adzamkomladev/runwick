import * as Dialog from '@radix-ui/react-dialog'
import { X } from 'lucide-react'
import { Brand } from './Brand'
import { SidebarNav } from './DocsSidebar'

export function MobileNav({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm lg:hidden overlay-motion" />
        <Dialog.Content aria-describedby={undefined} className="fixed inset-y-0 left-0 z-50 flex w-[min(86vw,340px)] flex-col border-r border-border bg-background shadow-float lg:hidden drawer-motion">
          <Dialog.Title className="sr-only">Documentation navigation</Dialog.Title>
          <div className="flex h-14 items-center justify-between border-b border-border px-4">
            <Brand />
            <Dialog.Close asChild>
              <button
                type="button"
                className="inline-flex size-9 items-center justify-center rounded-lg text-muted-foreground hover:bg-accent hover:text-foreground"
                aria-label="Close navigation"
              >
                <X className="size-5" />
              </button>
            </Dialog.Close>
          </div>
          <div className="scrollbar-thin flex-1 overflow-y-auto px-4 py-5">
            <SidebarNav onNavigate={() => onOpenChange(false)} />
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

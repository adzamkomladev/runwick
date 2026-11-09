import { Tabs as TabsPrimitive } from 'radix-ui'
import { cn } from './utils'

export const Tabs = ({ className, ...props }) => <TabsPrimitive.Root data-slot="tabs" className={cn('flex flex-col gap-3', className)} {...props} />
export const TabsList = ({ className, ...props }) =>
  <TabsPrimitive.List data-slot="tabs-list" className={cn('inline-flex h-9 w-fit items-center justify-center rounded-lg bg-muted p-[3px] text-muted-foreground', className)} {...props} />
export const TabsTrigger = ({ className, ...props }) => (
  <TabsPrimitive.Trigger data-slot="tabs-trigger" className={cn(
    'inline-flex h-full flex-1 items-center justify-center gap-1.5 whitespace-nowrap rounded-md border border-transparent px-3 py-1 text-sm font-medium press outline-none hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50 data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm',
    className)} {...props} />
)
export const TabsContent = ({ className, ...props }) =>
  <TabsPrimitive.Content data-slot="tabs-content" className={cn('flex-1 outline-none data-[state=active]:animate-page-in', className)} {...props} />

import { IconChevronLeft, IconChevronRight } from '@tabler/icons-react'
import { DayPicker } from 'react-day-picker'
import { buttonVariants } from './button'
import { cn } from './utils'

/** react-day-picker (MIT) styled with our tokens; its own stylesheet is not loaded (the CSP and the look are ours). */
export function Calendar({ className, ...props }) {
  return (
    <DayPicker showOutsideDays className={cn('select-none', className)}
      classNames={{
        months: 'relative flex flex-col gap-4 sm:flex-row',
        month: 'grid gap-3',
        month_caption: 'flex h-8 items-center justify-center text-sm font-medium',
        nav: 'absolute inset-x-0 top-0 flex items-center justify-between',
        button_previous: cn(buttonVariants({ variant: 'ghost', size: 'icon-sm' }), 'size-8'),
        button_next: cn(buttonVariants({ variant: 'ghost', size: 'icon-sm' }), 'size-8'),
        month_grid: 'border-collapse',
        weekdays: 'flex',
        weekday: 'w-9 text-[0.75rem] font-normal text-muted-foreground',
        week: 'mt-1 flex',
        day: 'relative size-9 p-0 text-center text-sm',
        day_button: 'size-9 rounded-md tabular-nums outline-none press hover:bg-accent focus-visible:ring-[3px] focus-visible:ring-ring/50',
        selected: '[&>button]:bg-primary [&>button]:text-primary-foreground [&>button]:hover:bg-primary',
        range_start: 'rounded-l-md bg-accent',
        range_end: 'rounded-r-md bg-accent',
        range_middle: 'bg-accent [&>button]:!bg-transparent [&>button]:!text-accent-foreground',
        today: '[&>button]:font-semibold [&>button]:underline [&>button]:decoration-highlight [&>button]:decoration-2 [&>button]:underline-offset-4',
        outside: 'text-muted-foreground/60',
        disabled: 'pointer-events-none opacity-40',
        hidden: 'invisible',
      }}
      components={{ Chevron: ({ orientation }) => (orientation === 'left' ? <IconChevronLeft className="size-4" /> : <IconChevronRight className="size-4" />) }}
      {...props} />
  )
}

import { cn } from './utils'

export const Table = ({ className, ...props }) => (
  <div data-slot="table-container" className="relative w-full overflow-x-auto">
    <table data-slot="table" className={cn('w-full caption-bottom text-sm', className)} {...props} />
  </div>
)
export const TableHeader = ({ className, ...props }) => <thead data-slot="table-header" className={cn('[&_tr]:border-b', className)} {...props} />
export const TableBody = ({ className, ...props }) => <tbody data-slot="table-body" className={cn('[&_tr:last-child]:border-0', className)} {...props} />
export const TableRow = ({ className, ...props }) =>
  <tr data-slot="table-row" className={cn('border-b transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted', className)} {...props} />
export const TableHead = ({ className, ...props }) =>
  <th data-slot="table-head" className={cn('h-10 whitespace-nowrap px-3 text-left align-middle text-xs font-medium uppercase tracking-wide text-muted-foreground', className)} {...props} />
export const TableCell = ({ className, ...props }) => <td data-slot="table-cell" className={cn('h-11 px-3 py-1.5 align-middle', className)} {...props} />

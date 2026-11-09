import { IconMinus, IconPlus } from '@tabler/icons-react'
import { useId } from 'react'
import { cn } from './utils'

/**
 * A whole-number field without the browser's number spinner: a text input (inputmode numeric) with ARIA spinbutton semantics,
 * ArrowUp/ArrowDown and PageUp/PageDown steps, and − / + buttons. `value` is a string ('' = empty); `onChange` gets the string.
 */
export function NumberField({ value, onChange, min = 0, max, step = 1, suffix, placeholder, className, id, ...props }) {
  const auto = useId()
  const n = value === '' || value == null ? null : Number(value)
  const clamp = (v) => Math.min(max ?? Infinity, Math.max(min, v))
  const bump = (d) => onChange(String(clamp((n ?? min) + d)))
  const onKey = (e) => {
    const d = { ArrowUp: step, ArrowDown: -step, PageUp: step * 10, PageDown: -step * 10 }[e.key]
    if (d) { e.preventDefault(); bump(d) }
  }
  return (
    <div className={cn('flex h-9 w-full items-center rounded-md border border-input bg-card shadow-xs transition-[color,box-shadow] focus-within:border-ring focus-within:ring-[3px] focus-within:ring-ring/50', className)}>
      <button type="button" tabIndex={-1} aria-hidden onClick={() => bump(-step)} className="grid h-full w-8 shrink-0 place-items-center text-muted-foreground hover:text-foreground disabled:opacity-40" disabled={n != null && n <= min}><IconMinus className="size-3.5" /></button>
      <input id={id ?? auto} type="text" inputMode="numeric" role="spinbutton" aria-valuenow={n ?? undefined} aria-valuemin={min} aria-valuemax={max}
        value={value ?? ''} placeholder={placeholder} onKeyDown={onKey} onChange={(e) => onChange(e.target.value.replace(/[^\d]/g, ''))}
        className="h-full w-full min-w-0 bg-transparent text-center text-sm tabular-nums outline-none placeholder:text-muted-foreground/70" {...props} />
      {suffix && <span className="pr-1 text-xs text-muted-foreground">{suffix}</span>}
      <button type="button" tabIndex={-1} aria-hidden onClick={() => bump(step)} className="grid h-full w-8 shrink-0 place-items-center text-muted-foreground hover:text-foreground disabled:opacity-40" disabled={n != null && max != null && n >= max}><IconPlus className="size-3.5" /></button>
    </div>
  )
}

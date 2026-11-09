import { cn } from './ui/utils'

// One colour per state, the same in every table, badge and timeline node. Text and tint pairs meet WCAG AA in both themes
// (700-on-50 light, 300-on-tinted dark). Live states (running, reserved) pulse; nothing else moves.
export const TONES = {
  neutral: { badge: 'bg-zinc-100 text-zinc-700 ring-zinc-500/20 dark:bg-zinc-400/10 dark:text-zinc-300 dark:ring-zinc-400/25', dot: 'bg-zinc-400' },
  blue: { badge: 'bg-sky-50 text-sky-800 ring-sky-600/20 dark:bg-sky-400/10 dark:text-sky-300 dark:ring-sky-400/25', dot: 'bg-sky-500' },
  indigo: { badge: 'bg-indigo-50 text-indigo-700 ring-indigo-600/20 dark:bg-indigo-400/10 dark:text-indigo-300 dark:ring-indigo-400/30', dot: 'bg-indigo-500' },
  amber: { badge: 'bg-amber-50 text-amber-800 ring-amber-600/25 dark:bg-amber-400/10 dark:text-amber-300 dark:ring-amber-400/25', dot: 'bg-amber-500' },
  green: { badge: 'bg-emerald-50 text-emerald-800 ring-emerald-600/20 dark:bg-emerald-400/10 dark:text-emerald-300 dark:ring-emerald-400/25', dot: 'bg-emerald-500' },
  red: { badge: 'bg-rose-50 text-rose-700 ring-rose-600/20 dark:bg-rose-400/10 dark:text-rose-300 dark:ring-rose-400/25', dot: 'bg-rose-500' },
}

const STATES = {
  pending: ['neutral'], reserved: ['blue', true], running: ['indigo', true], retrying: ['amber'], succeeded: ['green'],
  failed: ['red'], cancelled: ['neutral'], paused: ['amber'], active: ['green'], live: ['green'], draining: ['amber'],
  expired: ['neutral'], permanent_failure: ['red'], retryable_failure: ['amber'], user_cancelled: ['neutral'], interrupted: ['amber'],
  timed_out: ['red'],
}

export const toneOf = (state) => TONES[STATES[state]?.[0] ?? 'neutral']

/** A dot that pulses for a live state. */
export function StatusDot({ state, className }) {
  const [tone, live] = STATES[state] ?? ['neutral']
  const dot = TONES[tone].dot
  return (
    <span aria-hidden className={cn('relative flex size-1.5 shrink-0', className)}>
      {live && <span className={cn('absolute inline-flex size-full rounded-full opacity-75 motion-safe:animate-ping', dot)} />}
      <span className={cn('relative inline-flex size-1.5 rounded-full', dot)} />
    </span>
  )
}

/** A state as a dot and its name on a tinted chip. The text is the state itself, so it never relies on colour alone. */
export function StatusBadge({ state, className, children }) {
  return (
    <span data-slot="status" className={cn('inline-flex h-6 w-fit shrink-0 items-center gap-1.5 rounded-md px-2 text-xs font-medium whitespace-nowrap ring-1 ring-inset transition-colors duration-500',
      toneOf(state).badge, className)}>
      <StatusDot state={state} />
      {children ?? state.replaceAll('_', ' ')}
    </span>
  )
}

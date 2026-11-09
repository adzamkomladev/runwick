import { cn } from './ui/utils'

// One colour per state, the same in every table, badge and timeline node. Text and tint pairs meet WCAG AA in both themes
// (700/800-on-tint light, 200-400-on-tint dark), all from the Runwick palette. Live states (running, reserved) pulse; nothing else moves.
export const TONES = {
  neutral: { badge: 'bg-midnight-violet-100 text-midnight-violet-700 ring-midnight-violet-600/20 dark:bg-midnight-violet-400/10 dark:text-midnight-violet-200 dark:ring-midnight-violet-400/25', dot: 'bg-midnight-violet-400' },
  blue: { badge: 'bg-pine-blue-50 text-pine-blue-700 ring-pine-blue-600/20 dark:bg-pine-blue-400/10 dark:text-pine-blue-300 dark:ring-pine-blue-400/25', dot: 'bg-pine-blue-400' },
  indigo: { badge: 'bg-pine-blue-100 text-pine-blue-800 ring-pine-blue-600/30 dark:bg-pine-blue-400/15 dark:text-pine-blue-200 dark:ring-pine-blue-400/30', dot: 'bg-pine-blue-600 dark:bg-pine-blue-300' },
  amber: { badge: 'bg-chocolate-50 text-chocolate-700 ring-chocolate-600/25 dark:bg-chocolate-400/10 dark:text-chocolate-300 dark:ring-chocolate-400/25', dot: 'bg-chocolate-400' },
  green: { badge: 'bg-lime-yellow-100 text-lime-yellow-800 ring-lime-yellow-700/25 dark:bg-lime-yellow-400/10 dark:text-lime-yellow-400 dark:ring-lime-yellow-400/25', dot: 'bg-lime-yellow-600 dark:bg-lime-yellow-500' },
  red: { badge: 'bg-chocolate-100 text-chocolate-800 ring-chocolate-700/30 dark:bg-chocolate-500/15 dark:text-chocolate-200 dark:ring-chocolate-400/30', dot: 'bg-chocolate-600 dark:bg-chocolate-400' },
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
      <span className={cn('relative inline-flex size-1.5 rounded-full transition-colors duration-200', dot)} />
    </span>
  )
}

/** A state as a dot and its name on a tinted chip. The text is the state itself, so it never relies on colour alone. */
export function StatusBadge({ state, className, children }) {
  return (
    <span data-slot="status" className={cn('inline-flex h-6 w-fit shrink-0 items-center gap-1.5 rounded-md px-2 text-xs font-medium whitespace-nowrap ring-1 ring-inset transition-[color,background-color,box-shadow] duration-200',
      toneOf(state).badge, className)}>
      <StatusDot state={state} />
      {children ?? state.replaceAll('_', ' ')}
    </span>
  )
}

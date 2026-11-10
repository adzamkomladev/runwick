import { AlertTriangle, CircleCheck, Info, Lightbulb, OctagonAlert } from 'lucide-react'
import type { CSSProperties, ReactNode } from 'react'

// One hue per job; the tint and hairline come from the same token so both themes stay in step.
const types = {
  info: { icon: Info, label: 'Note', tone: 'var(--info)' },
  tip: { icon: Lightbulb, label: 'Tip', tone: 'var(--success)' },
  warning: { icon: AlertTriangle, label: 'Warning', tone: 'var(--warning)' },
  danger: { icon: OctagonAlert, label: 'Important', tone: 'var(--danger)' },
  success: { icon: CircleCheck, label: 'Success', tone: 'var(--success)' },
} as const

export function Callout({
  type = 'info',
  title,
  children,
}: {
  type?: keyof typeof types
  title?: string
  children: ReactNode
}) {
  const config = types[type] ?? types.info
  const Icon = config.icon

  return (
    <div
      className="not-prose my-7 flex gap-3 rounded-xl border border-[color-mix(in_oklab,var(--tone)_28%,var(--border))] bg-[color-mix(in_oklab,var(--tone)_6%,var(--background))] px-4 py-3.5 text-sm leading-6 text-foreground [&_ul]:my-2 [&_ul]:list-disc [&_ul]:space-y-1 [&_ul]:pl-5"
      style={{ '--tone': config.tone } as CSSProperties}
    >
      <Icon className="mt-[3px] size-[18px] shrink-0 text-[var(--tone)]" />
      <div className="min-w-0 flex-1 text-[color-mix(in_oklab,var(--foreground)_88%,var(--background))] [&>p]:m-0 [&>p+p]:mt-3 [&_a]:font-medium [&_a]:underline [&_a]:underline-offset-4 [&_:not(pre)>code]:rounded-md [&_:not(pre)>code]:border [&_:not(pre)>code]:border-[color-mix(in_oklab,var(--tone)_25%,var(--border))] [&_:not(pre)>code]:bg-[color-mix(in_oklab,var(--tone)_8%,var(--background))] [&_:not(pre)>code]:px-1 [&_:not(pre)>code]:py-0.5 [&_:not(pre)>code]:font-mono [&_:not(pre)>code]:text-[0.85em]">
        <p className="mb-0.5 font-semibold">{title ?? config.label}</p>
        {children}
      </div>
    </div>
  )
}

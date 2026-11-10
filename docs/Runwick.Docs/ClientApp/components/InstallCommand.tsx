import { Check, Copy } from 'lucide-react'
import { useState } from 'react'

export function InstallCommand({ command }: { command: string }) {
  const [copied, setCopied] = useState(false)

  return (
    <button
      type="button"
      onClick={async () => {
        try {
          await navigator.clipboard.writeText(command)
          setCopied(true)
          window.setTimeout(() => setCopied(false), 1600)
        } catch {
          // Leave the command selectable when the clipboard is unavailable.
        }
      }}
      className="group flex w-full items-center gap-2 rounded-md border border-border bg-card px-3.5 py-2.5 text-left font-mono text-[12.5px] transition-colors hover:border-foreground/30 sm:w-auto"
      aria-label="Copy install command"
    >
      <span className="select-none text-muted-foreground">$</span>
      <span className="min-w-0 flex-1 truncate">{command}</span>
      {copied ? <Check className="size-4 shrink-0 text-success" /> : <Copy className="size-4 shrink-0 text-muted-foreground/60 group-hover:text-foreground" />}
    </button>
  )
}

import { Check, Copy } from 'lucide-react'
import { useEffect, useRef, useState, type ComponentProps } from 'react'
import { cn } from '../lib/utils'

const labels: Record<string, string> = {
  cs: 'C#',
  csharp: 'C#',
  ts: 'TypeScript',
  tsx: 'TSX',
  js: 'JavaScript',
  jsx: 'JSX',
  sh: 'Shell',
  bash: 'Bash',
  shell: 'Shell',
  json: 'JSON',
  jsonc: 'JSON',
  html: 'HTML',
  xml: 'XML',
  css: 'CSS',
  diff: 'Diff',
  yaml: 'YAML',
  yml: 'YAML',
  ini: 'INI',
  text: 'Text',
  http: 'HTTP',
}

export function CodeBlock({ className, children, ...props }: ComponentProps<'pre'>) {
  const ref = useRef<HTMLPreElement>(null)
  const [copied, setCopied] = useState(false)
  const [language, setLanguage] = useState(() => className?.match(/language-([\w-]+)/)?.[1] ?? 'text')

  useEffect(() => {
    const codeLanguage = ref.current?.querySelector('code')?.className.match(/language-([\w-]+)/)?.[1]
    if (codeLanguage) setLanguage(codeLanguage)
  }, [children])

  // Fade the edge that hides more code so cut-off lines read as scrollable.
  const scrollRef = useRef<HTMLDivElement>(null)
  const [overflow, setOverflow] = useState<'none' | 'start' | 'end' | 'both'>('none')
  const measure = () => {
    const element = scrollRef.current
    if (!element) return
    const start = element.scrollLeft > 2
    const end = element.scrollLeft + element.clientWidth < element.scrollWidth - 2
    setOverflow(start && end ? 'both' : start ? 'start' : end ? 'end' : 'none')
  }

  useEffect(() => {
    measure()
    window.addEventListener('resize', measure)
    return () => window.removeEventListener('resize', measure)
  }, [])

  const copy = async () => {
    const text = ref.current?.querySelector('code')?.textContent ?? ''
    try {
      await navigator.clipboard.writeText(text.replace(/\n$/, ''))
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      // Clipboard access can be denied; the code remains selectable.
    }
  }

  return (
    <figure className="code-frame not-prose group/code my-7 overflow-hidden rounded-xl border border-border bg-code shadow-[0_1px_2px_oklch(0.21_0.03_329/0.04)]">
      <figcaption className="flex h-10 items-center justify-between border-b border-border bg-muted/50 pl-4 pr-1.5">
        <span className="font-mono text-[11px] font-medium tracking-wide text-muted-foreground">
          {labels[language] ?? language}
        </span>
        <button
          type="button"
          onClick={copy}
          className={cn(
            'relative inline-flex h-7 items-center gap-1.5 overflow-hidden rounded-md px-2 text-[11px] font-medium transition-[color,background-color] duration-150 active:scale-95',
            copied ? 'text-success' : 'text-muted-foreground hover:bg-accent hover:text-foreground',
          )}
          aria-label={copied ? 'Copied' : 'Copy code'}
        >
          <span className="relative grid size-3.5 place-items-center">
            <Copy className={cn('absolute size-3.5 transition-[opacity,transform] duration-200', copied ? 'scale-50 opacity-0' : 'scale-100 opacity-100')} />
            <Check className={cn('absolute size-3.5 transition-[opacity,transform] duration-300 ease-[var(--ease-out)]', copied ? 'scale-100 opacity-100' : 'scale-50 opacity-0')} strokeWidth={2.5} />
          </span>
          <span aria-live="polite">{copied ? 'Copied' : 'Copy'}</span>
        </button>
      </figcaption>
      <div ref={scrollRef} onScroll={measure} data-overflow={overflow} className="code-scroll scrollbar-thin overflow-x-auto">
        <pre ref={ref} className={cn(className, 'm-0 rounded-none bg-transparent py-4')} {...props}>
          {children}
        </pre>
      </div>
    </figure>
  )
}

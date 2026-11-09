import { useCallback, useId, useState } from 'react'
import { cn } from './ui/utils'
import { AnimatedNumber } from '../lib'

// Charts in the shadcn/ui chart style (grid, axis labels, legend, hover cursor and tooltip), drawn as SVG in pixel space.
// Colours come from the --chart-n tokens through currentColor. Sizes are measured with a ResizeObserver; geometry set through
// React props only (no style element, no inline style in HTML), so the CSP holds.

function useWidth() {
  const [width, setWidth] = useState(0)
  const ref = useCallback((el) => {
    if (!el) return undefined
    const ro = new ResizeObserver(([e]) => setWidth(Math.round(e.contentRect.width)))
    ro.observe(el)
    return () => ro.disconnect()
  }, [])
  return [ref, width]
}

const niceMax = (v) => {
  if (v <= 4) return 4
  const p = 10 ** Math.floor(Math.log10(v)), n = v / p
  return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * p
}

/** Monotone cubic path through the points (no overshoot below zero). */
function smooth(pts) {
  if (pts.length < 2) return pts.length ? `M${pts[0][0]},${pts[0][1]}` : ''
  const d = [`M${pts[0][0]},${pts[0][1]}`]
  for (let i = 0; i < pts.length - 1; i++) {
    const [x0, y0] = pts[i], [x1, y1] = pts[i + 1], mx = (x1 - x0) / 2
    d.push(`C${x0 + mx},${y0} ${x1 - mx},${y1} ${x1},${y1}`)
  }
  return d.join(' ')
}

export function Legend({ series }) {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {series.map((s) => <span key={s.key} className="inline-flex items-center gap-1.5"><span className={cn('size-2.5 rounded-[3px] bg-current', s.color)} /><span>{s.label}</span></span>)}
    </div>
  )
}

function Tip({ x, width, label, rows }) {
  const left = Math.min(Math.max(x + 12, 0), Math.max(width - 168, 0))
  return (
    <div className="pointer-events-none absolute top-1 z-10 grid min-w-36 gap-1 rounded-lg border bg-popover/95 px-3 py-2 text-xs text-popover-foreground shadow-lg backdrop-blur-sm transition-[left] duration-100" ref={(el) => { if (el) el.style.left = `${left}px` }}>
      <span className="font-medium">{label}</span>
      {rows.map((r) => (
        <span key={r.label} className="flex items-center justify-between gap-4">
          <span className="inline-flex items-center gap-1.5 text-muted-foreground"><span className={cn('size-2 rounded-[2px] bg-current', r.color)} />{r.label}</span>
          <span className="font-mono font-medium tabular-nums">{r.value}</span>
        </span>
      ))}
    </div>
  )
}

const PAD = { top: 12, right: 8, bottom: 22, left: 30 }

/**
 * Area or bar chart over `data` (one object per x step, with `label` and a number per series key). `stacked` stacks bars.
 */
export function Chart({ data, series, type = 'area', stacked = false, height = 220, label, className }) {
  const [ref, width] = useWidth()
  const [hover, setHover] = useState(null)
  const box = useCallback((el) => { if (el) el.style.minHeight = `${height}px`; return ref(el) }, [ref, height])
  const id = 'g' + useId().replace(/[^a-zA-Z0-9]/g, '')
  const w = Math.max(width - PAD.left - PAD.right, 1), h = height - PAD.top - PAD.bottom
  const totals = data.map((d) => (stacked ? series.reduce((t, s) => t + (d[s.key] ?? 0), 0) : Math.max(...series.map((s) => d[s.key] ?? 0))))
  const max = niceMax(Math.max(...totals, 0))
  const step = data.length ? w / data.length : w
  const x = (i) => PAD.left + (type === 'bar' ? step * i + step / 2 : data.length > 1 ? (w * i) / (data.length - 1) : w / 2)
  const y = (v) => PAD.top + h - (v / max) * h
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * max)
  const every = Math.max(1, Math.ceil(data.length / Math.max(2, Math.floor(w / 70))))
  const move = (e) => {
    const r = e.currentTarget.getBoundingClientRect()
    const px = e.clientX - r.left - PAD.left
    const i = type === 'bar' ? Math.floor(px / step) : Math.round((px / w) * (data.length - 1))
    setHover(i >= 0 && i < data.length ? i : null)
  }
  return (
    // The box keeps its height before it is measured, so the chart never grows into place (no layout shift).
    <div ref={box} data-slot="chart" className={cn('relative w-full', className)}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={label} className="block overflow-visible"
          onPointerMove={move} onPointerLeave={() => setHover(null)}>
          {ticks.map((t) => (
            <g key={t}>
              <line x1={PAD.left} x2={PAD.left + w} y1={y(t)} y2={y(t)} className="stroke-border" strokeDasharray={t ? '3 3' : undefined} />
              <text x={PAD.left - 6} y={y(t)} dy="0.32em" textAnchor="end" className="fill-muted-foreground text-[10px] tabular-nums">{Math.round(t)}</text>
            </g>
          ))}
          {data.map((d, i) => i % every === 0 && (
            <text key={i} x={x(i)} y={height - 6} textAnchor="middle" className="fill-muted-foreground text-[10px]">{d.label}</text>
          ))}
          {type === 'area' && series.map((s) => {
            const pts = data.map((d, i) => [x(i), y(d[s.key] ?? 0)])
            const line = smooth(pts)
            return (
              <g key={s.key} className={s.color}>
                <defs>
                  <linearGradient id={`${id}-${s.key}`} x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor="currentColor" stopOpacity="0.3" />
                    <stop offset="100%" stopColor="currentColor" stopOpacity="0.02" />
                  </linearGradient>
                </defs>
                {pts.length > 1 && <path d={`${line} L${pts.at(-1)[0]},${y(0)} L${pts[0][0]},${y(0)} Z`} fill={`url(#${id}-${s.key})`} className="animate-chart-in chart-morph" />}
                <path d={line} fill="none" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" className="animate-chart-in chart-morph" />
                {hover != null && <circle cx={x(hover)} cy={y(data[hover][s.key] ?? 0)} r="4" fill="currentColor" className="stroke-card chart-morph" strokeWidth="2" />}
              </g>
            )
          })}
          {type === 'bar' && data.map((d, i) => {
            const bw = Math.max(Math.min(step * (stacked ? 0.62 : 0.72) / (stacked ? 1 : series.length), 28), 2)
            let base = 0
            return (
              <g key={i} className={cn('transition-opacity duration-150', hover != null && hover !== i && 'opacity-45')}>
                {series.map((s, k) => {
                  const v = d[s.key] ?? 0
                  const top = stacked ? y(base + v) : y(v)
                  const bottom = stacked ? y(base) : y(0)
                  const left = stacked ? x(i) - bw / 2 : x(i) - (bw * series.length) / 2 + bw * k
                  base += v
                  return v > 0 && <rect key={s.key} x={left} y={top} width={bw} height={Math.max(bottom - top, 1)} rx="3" fill="currentColor" className={cn(s.color, 'animate-bar-in origin-bottom chart-morph')} />
                })}
              </g>
            )
          })}
          {hover != null && type === 'area' && <line x1={x(hover)} x2={x(hover)} y1={PAD.top} y2={PAD.top + h} className="stroke-muted-foreground/40" strokeDasharray="3 3" />}
        </svg>
      )}
      {hover != null && data[hover] && <Tip x={x(hover)} width={width} label={data[hover].title ?? data[hover].label}
        rows={series.map((s) => ({ label: s.label, color: s.color, value: data[hover][s.key] ?? 0 }))} />}
    </div>
  )
}

/** A ranked list with proportional bars: "failures by job", "pending by queue". */
export function BarList({ items, color = 'text-chart-1', empty }) {
  const max = Math.max(...items.map((i) => i.value), 1)
  if (!items.length) return <p className="py-6 text-center text-sm text-muted-foreground">{empty}</p>
  return (
    <ul className="grid gap-2">
      {items.map((i) => (
        <li key={i.label} className="group/bar grid grid-cols-[1fr_auto] items-center gap-3 text-sm">
          <span className="relative h-8 min-w-0 overflow-hidden rounded-md">
            <span data-slot="bar" className={cn('absolute inset-y-0 left-0 rounded-md bg-current opacity-15 transition-[width,opacity] duration-500 ease-out group-hover/bar:opacity-25', color)}
              ref={(el) => { if (el) el.style.width = `${Math.max((i.value / max) * 100, 2)}%` }} />
            <span className="relative flex h-full items-center truncate px-2.5">{i.label}</span>
          </span>
          <span className="font-medium tabular-nums">{i.display ?? <AnimatedNumber value={i.value} />}</span>
        </li>
      ))}
    </ul>
  )
}

/** A thin full-width area with no axes, for KPI cards. */
export function Spark({ data, color, label }) {
  const [ref, width] = useWidth()
  const id = 'g' + useId().replace(/[^a-zA-Z0-9]/g, '')
  const height = 44
  const pts = data.length === 1 ? [data[0], data[0]] : data
  const max = Math.max(...pts, 1), min = Math.min(...pts, 0)
  const xy = pts.map((v, i) => [(i / Math.max(pts.length - 1, 1)) * width, 4 + (height - 8) * (1 - (v - min) / (max - min || 1))])
  const line = smooth(xy)
  return (
    <div ref={ref} data-slot="chart" className={cn('h-11 w-full', color)}>
      {width > 0 && pts.length > 0 && (
        <svg width={width} height={height} role="img" aria-label={label} className="block overflow-visible">
          <defs><linearGradient id={id} x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stopColor="currentColor" stopOpacity="0.28" /><stop offset="100%" stopColor="currentColor" stopOpacity="0" /></linearGradient></defs>
          <path d={`${line} L${width},${height} L0,${height} Z`} fill={`url(#${id})`} className="chart-morph" />
          <path d={line} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" className="animate-chart-in chart-morph" />
        </svg>
      )}
    </div>
  )
}

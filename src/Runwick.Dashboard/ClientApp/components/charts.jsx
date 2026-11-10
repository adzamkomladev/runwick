import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { cn } from './ui/utils'
import { AnimatedNumber } from '../lib'

// Charts in the shadcn/ui chart style (grid, axis labels, legend, hover cursor and tooltip), drawn as SVG in pixel space.
// Colours come from the --chart-n tokens through currentColor. Sizes are measured with a ResizeObserver; geometry set through
// React props or the CSSOM (ref callbacks), never a style attribute in the markup, so the CSP holds.
//
// Motion (21 §4.7): on mount lines trace in, areas rise, bars grow, staggered; on each poll the values tween from the old to the
// new numbers (useTween) so the shapes glide instead of redrawing; the cursor, dots and tooltip glide on transforms. Under
// reduced motion everything is drawn at once.

const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)')

/** The numbers in `target`, eased from their previous values over `ms` (a jump when the length changes or motion is reduced). */
export function useTween(target, ms = 450) {
  const [shown, setShown] = useState(target)
  const current = useRef(target)
  const key = target.join(',')
  useEffect(() => {
    const from = current.current
    if (reducedMotion.matches || from.length !== target.length) {
      current.current = target
      setShown(target)
      return undefined
    }
    const start = performance.now()
    let frame = 0
    const step = (now) => {
      const t = Math.min((now - start) / ms, 1), e = 1 - (1 - t) ** 3
      const v = target.map((b, i) => from[i] + (b - from[i]) * e)
      current.current = v
      setShown(v)
      if (t < 1) frame = requestAnimationFrame(step)
    }
    frame = requestAnimationFrame(step)
    return () => cancelAnimationFrame(frame)
  }, [key]) // eslint-disable-line react-hooks/exhaustive-deps -- the values are the key
  return shown
}

/** A ref callback that sets one CSS property through the CSSOM (allowed by the CSP, unlike a style attribute). */
const css = (prop, value) => (el) => { if (el) el.style[prop] = value }

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

/** The hovered index, kept after the pointer leaves so the cursor and tooltip can fade out where they were. */
function useHover() {
  const [state, setState] = useState({ i: null, on: false })
  return [state, (i) => setState((s) => (i == null ? { ...s, on: false } : { i, on: true }))]
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

function Tip({ x, width, on, label, rows }) {
  const left = Math.min(Math.max(x + 12, 0), Math.max(width - 168, 0))
  return (
    <div data-testid="chart-tip" className={cn('pointer-events-none absolute top-1 left-0 z-10 grid min-w-36 gap-1 rounded-lg border bg-popover/95 px-3 py-2 text-xs text-popover-foreground shadow-lg backdrop-blur-sm glide',
      on ? 'opacity-100' : 'opacity-0')} ref={css('transform', `translate3d(${left}px,0,0)`)}>
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
  const [hover, setHover] = useHover()
  const box = useCallback((el) => { if (el) el.style.minHeight = `${height}px`; return ref(el) }, [ref, height])
  const id = 'g' + useId().replace(/[^a-zA-Z0-9]/g, '')
  const n = data.length
  const raw = series.flatMap((s) => data.map((d) => d[s.key] ?? 0))
  const totals = data.map((d) => (stacked ? series.reduce((t, s) => t + (d[s.key] ?? 0), 0) : Math.max(...series.map((s) => d[s.key] ?? 0))))
  const tweened = useTween([...raw, niceMax(Math.max(...totals, 0))])
  const max = tweened.at(-1) || 1
  const value = (k, i) => tweened[k * n + i]
  const w = Math.max(width - PAD.left - PAD.right, 1), h = height - PAD.top - PAD.bottom
  const step = n ? w / n : w
  const x = (i) => PAD.left + (type === 'bar' ? step * i + step / 2 : n > 1 ? (w * i) / (n - 1) : w / 2)
  const y = (v) => PAD.top + h - (v / max) * h
  const axisMax = niceMax(Math.max(...totals, 0))
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * axisMax)
  const every = Math.max(1, Math.ceil(n / Math.max(2, Math.floor(w / 70))))
  const move = (e) => {
    const r = e.currentTarget.getBoundingClientRect()
    const px = e.clientX - r.left - PAD.left
    const i = type === 'bar' ? Math.floor(px / step) : Math.round((px / w) * (n - 1))
    setHover(i >= 0 && i < n ? i : null)
  }
  const hi = hover.i != null && hover.i < n ? hover.i : null
  return (
    // The box keeps its height before it is measured, so the chart never grows into place (no layout shift).
    <div ref={box} data-slot="chart" className={cn('relative w-full', className)}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label={label} className="block overflow-visible"
          onPointerMove={move} onPointerLeave={() => setHover(null)}>
          {ticks.map((t, k) => (
            <g key={k}>
              <line x1={PAD.left} x2={PAD.left + w} y1={y(t)} y2={y(t)} className="stroke-border" strokeDasharray={t ? '3 3' : undefined} />
              <text x={PAD.left - 6} y={y(t)} dy="0.32em" textAnchor="end" className="fill-muted-foreground text-[10px] tabular-nums">{Math.round(t)}</text>
            </g>
          ))}
          {data.map((d, i) => i % every === 0 && (
            <text key={i} x={x(i)} y={height - 6} textAnchor="middle" className="fill-muted-foreground text-[10px]">{d.label}</text>
          ))}
          {type === 'area' && series.map((s, k) => {
            const pts = data.map((_, i) => [x(i), y(value(k, i))])
            const line = smooth(pts)
            return (
              <g key={s.key} className={s.color}>
                <defs>
                  <linearGradient id={`${id}-${s.key}`} x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor="currentColor" stopOpacity="0.3" />
                    <stop offset="100%" stopColor="currentColor" stopOpacity="0.02" />
                  </linearGradient>
                </defs>
                {pts.length > 1 && <path d={`${line} L${pts.at(-1)[0]},${y(0)} L${pts[0][0]},${y(0)} Z`} fill={`url(#${id}-${s.key})`}
                  className="animate-rise" ref={css('animationDelay', `${120 + k * 90}ms`)} />}
                <path d={line} fill="none" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" pathLength="1"
                  className="animate-trace" ref={css('animationDelay', `${k * 90}ms`)} />
                {hi != null && <g className={cn('glide', hover.on ? 'opacity-100' : 'opacity-0')} ref={css('transform', `translate(${x(hi)}px,${y(value(k, hi))}px)`)}>
                  <circle r="4" fill="currentColor" className="stroke-card" strokeWidth="2" />
                </g>}
              </g>
            )
          })}
          {type === 'bar' && data.map((d, i) => {
            const bw = Math.max(Math.min(step * (stacked ? 0.62 : 0.72) / (stacked ? 1 : series.length), 28), 2)
            let base = 0
            return (
              <g key={i} className={cn('transition-opacity duration-200 ease-glide', hi != null && hover.on && hi !== i && 'opacity-45')}>
                {series.map((s, k) => {
                  const v = value(k, i)
                  const top = stacked ? y(base + v) : y(v)
                  const bottom = stacked ? y(base) : y(0)
                  const left = stacked ? x(i) - bw / 2 : x(i) - (bw * series.length) / 2 + bw * k
                  base += v
                  return (d[s.key] ?? 0) > 0 && <rect key={s.key} x={left} y={top} width={bw} height={Math.max(bottom - top, 1)} rx="3" fill="currentColor"
                    className={cn(s.color, 'animate-bar-in')} ref={css('animationDelay', `${Math.min(i * 18, 420)}ms`)} />
                })}
              </g>
            )
          })}
          {hi != null && type === 'area' && <g className={cn('glide', hover.on ? 'opacity-100' : 'opacity-0')} ref={css('transform', `translateX(${x(hi)}px)`)}>
            <line x1={0} x2={0} y1={PAD.top} y2={PAD.top + h} className="stroke-muted-foreground/40" strokeDasharray="3 3" />
          </g>}
        </svg>
      )}
      {hi != null && data[hi] && <Tip x={x(hi)} width={width} on={hover.on} label={data[hi].title ?? data[hi].label}
        rows={series.map((s) => ({ label: s.label, color: s.color, value: data[hi][s.key] ?? 0 }))} />}
    </div>
  )
}

/**
 * A horizontal bar from 0 to `fraction` of its track, drawn with transform scaleX (never width), so it grows on mount and
 * glides on each poll without layout work. `from` offsets it, for stacked segments.
 */
export function Bar({ fraction, from = 0, className }) {
  return <span data-slot="bar" aria-hidden className={cn('absolute inset-y-0 left-0 w-full origin-left animate-grow-x glide-slow', className)}
    ref={css('transform', `translateX(${from * 100}%) scaleX(${Math.max(fraction, 0)})`)} />
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
            <Bar fraction={Math.max(i.value / max, 0.02)} className={cn('rounded-md bg-current opacity-15 group-hover/bar:opacity-25', color)} />
            <span className="relative flex h-full items-center truncate px-2.5">{i.label}</span>
          </span>
          <span className="font-medium tabular-nums">{i.display ?? <AnimatedNumber value={i.value} />}</span>
        </li>
      ))}
    </ul>
  )
}

/** A thin full-width area with no axes, for KPI cards. */
export function Spark({ data, color, label, small = false }) {
  const [ref, width] = useWidth()
  const id = 'g' + useId().replace(/[^a-zA-Z0-9]/g, '')
  const height = small ? 32 : 44
  const pts = useTween(data.length === 1 ? [data[0], data[0]] : data)
  const max = Math.max(...pts, 1), min = Math.min(...pts, 0)
  const xy = pts.map((v, i) => [(i / Math.max(pts.length - 1, 1)) * width, 4 + (height - 8) * (1 - (v - min) / (max - min || 1))])
  const line = smooth(xy)
  return (
    <div ref={ref} data-slot="chart" className={cn(small ? 'h-8' : 'h-11', 'w-full', color)}>
      {width > 0 && pts.length > 0 && (
        <svg width={width} height={height} role="img" aria-label={label} className="block overflow-visible">
          <defs><linearGradient id={id} x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stopColor="currentColor" stopOpacity="0.28" /><stop offset="100%" stopColor="currentColor" stopOpacity="0" /></linearGradient></defs>
          <path d={`${line} L${width},${height} L0,${height} Z`} fill={`url(#${id})`} className="animate-rise" ref={css('animationDelay', '160ms')} />
          <path d={line} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" pathLength="1" className="animate-trace" />
        </svg>
      )}
    </div>
  )
}

import { useMemo, useRef, useState } from "react";
import type { Sample } from "../types";
import { fmtClock, fmtInt } from "../format";

const W = 600;
const H = 180;
const PAD_TOP = 8;
const PAD_BOTTOM = 4;
const MAX_WINDOW_MS = 5 * 60 * 1000;
const MIN_WINDOW_MS = 60 * 1000;

/** Events-per-minute over the last five minutes, with a crosshair tooltip. */
export function Sparkline({ samples, now }: { samples: Sample[]; now: number }) {
  const [hover, setHover] = useState<number | null>(null);
  const ref = useRef<SVGSVGElement>(null);

  const { points, max, windowMs } = useMemo(() => {
    const visible = samples.filter((s) => now - s.t <= MAX_WINDOW_MS);
    // Until five minutes of history exist, stretch what we have across the full width.
    const span = visible.length ? now - visible[0].t : 0;
    const win = Math.min(MAX_WINDOW_MS, Math.max(MIN_WINDOW_MS, span));
    const peak = Math.max(1, ...visible.map((s) => s.epm));
    const niceMax = peak * 1.15;
    return {
      max: niceMax,
      windowMs: win,
      points: visible.map((s) => ({
        s,
        x: W - ((now - s.t) / win) * W,
        y: H - PAD_BOTTOM - (s.epm / niceMax) * (H - PAD_TOP - PAD_BOTTOM),
      })),
    };
  }, [samples, now]);

  if (points.length < 2) {
    return <div className="chart-empty">Collecting samples… the chart fills in over the next few seconds.</div>;
  }

  const line = points.map((p, i) => `${i ? "L" : "M"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  const area = `${line} L${points[points.length - 1].x.toFixed(1)},${H - PAD_BOTTOM} L${points[0].x.toFixed(1)},${H - PAD_BOTTOM} Z`;
  const gridY = (frac: number) => H - PAD_BOTTOM - (max / 1.15) * frac * ((H - PAD_TOP - PAD_BOTTOM) / max);

  const onMove = (e: React.PointerEvent) => {
    const box = ref.current?.getBoundingClientRect();
    if (!box) return;
    const x = ((e.clientX - box.left) / box.width) * W;
    let best = 0;
    points.forEach((p, i) => {
      if (Math.abs(p.x - x) < Math.abs(points[best].x - x)) best = i;
    });
    setHover(best);
  };

  const h = hover !== null ? points[hover] : null;
  return (
    <div className="chart">
      <svg
        ref={ref}
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={`Events per minute over the last five minutes; latest ${fmtInt(points[points.length - 1].s.epm)}`}
        onPointerMove={onMove}
        onPointerLeave={() => setHover(null)}
      >
        {[0.5, 1].map((f) => (
          <line key={f} x1={0} x2={W} y1={gridY(f)} y2={gridY(f)} className="grid" vectorEffect="non-scaling-stroke" />
        ))}
        <line x1={0} x2={W} y1={H - PAD_BOTTOM} y2={H - PAD_BOTTOM} className="baseline" vectorEffect="non-scaling-stroke" />
        <path d={area} className="area" />
        <path d={line} className="line" vectorEffect="non-scaling-stroke" />
        {h && <line x1={h.x} x2={h.x} y1={0} y2={H - PAD_BOTTOM} className="cross" vectorEffect="non-scaling-stroke" />}
      </svg>
      {h && (
        <>
          <span className="marker" style={{ left: `${(h.x / W) * 100}%`, top: `${(h.y / H) * 100}%` }} />
          <div className="tooltip" style={{ left: `clamp(70px, ${(h.x / W) * 100}%, calc(100% - 70px))` }}>
            <span className="mono-label">{fmtClock(h.s.t)}</span>
            <span>{fmtInt(h.s.epm)} events/min</span>
          </div>
        </>
      )}
      <div className="chart-axis mono-label">
        <span>{windowMs >= MAX_WINDOW_MS ? "5 min ago" : `${Math.round(windowMs / 1000)}s ago`}</span>
        <span>now</span>
      </div>
    </div>
  );
}

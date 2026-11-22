import type { CSSProperties, ReactNode } from "react";

interface SkeletonProps {
  width?: number | string;
  height?: number | string;
  className?: string;
  style?: CSSProperties;
}

/**
 * One shimmering placeholder block. Always aria-hidden; wrap a group in
 * SkeletonRegion so screen readers still get a "Loading…" announcement.
 */
export function Skeleton({ width = "100%", height = 14, className = "", style }: SkeletonProps) {
  return (
    <span
      aria-hidden="true"
      className={`skeleton${className ? ` ${className}` : ""}`}
      style={{ display: "block", width, height, ...style }}
    />
  );
}

/** A stack of text-line placeholders, cycling through the given widths. */
export function SkeletonLines({ lines = 3, widths = ["100%", "92%", "60%"] }: { lines?: number; widths?: string[] }) {
  return (
    <span className="skeleton-lines">
      {Array.from({ length: lines }, (_, i) => (
        <Skeleton key={i} width={widths[i % widths.length]} height={12} />
      ))}
    </span>
  );
}

/** Accessible wrapper: announces the loading state while the shimmer stays aria-hidden. */
export function SkeletonRegion({ label = "Loading…", children }: { label?: string; children: ReactNode }) {
  return (
    <div className="skeleton-region" role="status" aria-live="polite" aria-busy="true">
      <span className="visually-hidden">{label}</span>
      {children}
    </div>
  );
}

/** Grid of placeholder blocks, used for card headers and chip rows. */
function Stack({ gap = 8, children }: { gap?: number; children: ReactNode }) {
  return (
    <span className="skeleton-stack" style={{ gap }}>
      {children}
    </span>
  );
}

/** Four KPI cards matching the .kpis grid on the Overview. */
export function KpiGridSkeleton() {
  return (
    <div className="kpis" aria-hidden="true">
      {Array.from({ length: 4 }, (_, i) => (
        <section key={i} className="card kpi">
          <Skeleton width={96} height={10} />
          <Skeleton width={120} height={34} className="skeleton-kpi-value" />
          <SkeletonLines lines={2} widths={["100%", "72%"]} />
        </section>
      ))}
    </div>
  );
}

/** The throughput chart card on the Overview. */
export function ChartCardSkeleton() {
  return (
    <section className="card chart-card" aria-hidden="true">
      <div className="card-head">
        <Stack>
          <Skeleton width={150} height={10} />
          <Skeleton width={230} height={12} />
        </Stack>
        <Skeleton width={76} height={10} />
      </div>
      <Skeleton height={220} />
    </section>
  );
}

/** The "How data flows" card on the Overview. */
export function FlowCardSkeleton() {
  return (
    <section className="card" aria-hidden="true">
      <div className="card-head">
        <Stack>
          <Skeleton width={130} height={10} />
          <Skeleton width={210} height={12} />
        </Stack>
      </div>
      <ol className="flow">
        {Array.from({ length: 5 }, (_, i) => (
          <li key={i} className="flow-stage">
            <Skeleton width={10} height={10} className="round" />
            <div className="flow-text">
              <Stack gap={6}>
                <Skeleton width={96} height={14} />
                <Skeleton width={170} height={12} />
              </Stack>
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}

/** Whole Overview while /stats has not answered yet. */
export function OverviewSkeleton() {
  return (
    <SkeletonRegion label="Loading the stream overview…">
      <KpiGridSkeleton />
      <div className="overview-grid">
        <ChartCardSkeleton />
        <FlowCardSkeleton />
      </div>
    </SkeletonRegion>
  );
}

/** Body of one Pipeline query card while that query has not reported a batch yet. */
export function QueryCardBodySkeleton() {
  return (
    <div aria-hidden="true">
      <Skeleton width={130} height={34} className="skeleton-kpi-value" />
      <div className="meter">
        <Skeleton height={8} />
        <div className="meter-label">
          <Skeleton width={180} height={12} />
        </div>
      </div>
      <div className="outputs">
        <Skeleton width={170} height={10} />
        <ul className="output-tags">
          {Array.from({ length: 4 }, (_, i) => (
            <li key={i}>
              <Skeleton width={72} height={20} className="pill-shape" />
            </li>
          ))}
        </ul>
      </div>
      <dl className="rows">
        {Array.from({ length: 5 }, (_, i) => (
          <RowSkeleton key={i} />
        ))}
      </dl>
    </div>
  );
}

/** The feature result grid while a lookup is in flight. */
export function FeatureResultSkeleton() {
  return (
    <SkeletonRegion label="Looking up features…">
      <div aria-hidden="true">
        <div className="result-meta">
          <Skeleton width={150} height={12} />
          <Skeleton width={120} height={12} />
          <Skeleton width={100} height={12} />
          <Skeleton width={96} height={26} className="pill-shape" />
        </div>
        <div className="result-grid">
          {[10, 10, 4].map((rows, i) => (
            <section key={i} className="card">
              <Skeleton width={110} height={10} />
              <dl className="rows">
                {Array.from({ length: rows }, (_, r) => (
                  <RowSkeleton key={r} />
                ))}
              </dl>
            </section>
          ))}
        </div>
      </div>
    </SkeletonRegion>
  );
}

/** The "Active now" chip row while /users/sample has not answered yet. */
export function ChipRowSkeleton() {
  return (
    <SkeletonRegion label="Loading active users…">
      <div className="chip-row">
        <span className="mono-label">Active now</span>
        <span className="chip-skeletons" aria-hidden="true">
          {Array.from({ length: 6 }, (_, i) => (
            <Skeleton key={i} width={64} height={26} className="pill-shape" />
          ))}
        </span>
      </div>
    </SkeletonRegion>
  );
}

/** Schema registry card body while /contracts has not answered yet. */
export function SchemaCardSkeleton() {
  return (
    <SkeletonRegion label="Loading the schema registry…">
      <div aria-hidden="true">
        <div className="pill-group">
          {Array.from({ length: 3 }, (_, i) => (
            <Skeleton key={i} width={56} height={26} className="pill-shape" />
          ))}
        </div>
        <SkeletonLines lines={2} widths={["100%", "64%"]} />
        <div className="table-wrap">
          <table>
            <tbody>
              {Array.from({ length: 6 }, (_, i) => (
                <tr key={i}>
                  <td className="mono">
                    <Skeleton width={90 + (i % 3) * 24} height={12} />
                  </td>
                  <td className="mono muted">
                    <Skeleton width={70} height={12} />
                  </td>
                  <td>
                    <Skeleton width={80} height={16} className="pill-shape" />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </SkeletonRegion>
  );
}

/** Dead-letter queue card body while /dlq has not answered yet. */
export function DlqCardSkeleton() {
  return (
    <SkeletonRegion label="Loading the dead-letter queue…">
      <div aria-hidden="true">
        <div className="dlq-count">
          <Skeleton width={64} height={34} className="skeleton-kpi-value" />
          <Skeleton width={110} height={10} />
        </div>
        <ul className="dlq-list">
          {Array.from({ length: 3 }, (_, i) => (
            <li key={i}>
              <div className="dlq-row">
                <Skeleton width={220 - i * 40} height={12} />
                <Skeleton width={70} height={10} />
              </div>
              <Skeleton height={30} />
            </li>
          ))}
        </ul>
      </div>
    </SkeletonRegion>
  );
}

/** One label/value row of skeletons, matching the .rows grid. */
function RowSkeleton() {
  return (
    <>
      <dt>
        <Skeleton width={96} height={12} />
      </dt>
      <dd>
        <Skeleton width={56} height={12} />
      </dd>
    </>
  );
}

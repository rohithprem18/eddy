import { QUERY_NAMES, batchAge } from "../hooks";
import type { QueryName, Stats } from "../types";
import { fmt1, fmtAgo, fmtInt, fmtMs } from "../format";

const WINDOW_OUTPUTS = ["events", "views", "cart adds", "purchases", "spend", "avg order", "conversion", "refunds", "distinct products", "distinct categories"];

const ABOUT: Record<QueryName, { title: string; purpose: string; window: string; outputs: string[]; suffix: string }> = {
  window_5m: {
    title: "5-minute window",
    purpose: "Short-term intent: what each user did in the last few minutes.",
    window: "5 min window · slides every 1 min",
    outputs: WINDOW_OUTPUTS,
    suffix: "_5m",
  },
  window_1h: {
    title: "1-hour window",
    purpose: "Session-level behaviour: spend, conversion and breadth over the hour.",
    window: "1 h window · slides every 10 min",
    outputs: WINDOW_OUTPUTS,
    suffix: "_1h",
  },
  profile: {
    title: "Profile",
    purpose: "Last-seen attributes: latest event, product, category and device.",
    window: "Stateless · latest event per user",
    outputs: ["last event", "last category", "last product", "last device", "last seen"],
    suffix: "",
  },
};

function utilization(input: number, processed: number): number | null {
  if (!processed) return null;
  return Math.min(1.5, input / processed);
}

export function Pipeline({ stats }: { stats: Stats | null }) {
  return (
    <div className="view pipeline">
      <header className="view-head">
        <p className="eyebrow">Spark Structured Streaming</p>
        <h1>Three queries, one stream.</h1>
        <p className="lead">
          Each query reads the same Kafka topic and keeps its own state. Healthy means it processes events faster than
          they arrive.
        </p>
      </header>

      <div className="query-grid">
        {QUERY_NAMES.map((name) => {
          const q = stats?.queries[name] ?? null;
          const about = ABOUT[name];
          const util = q ? utilization(q.input_rows_per_second, q.processed_rows_per_second) : null;
          const age = stats ? batchAge(stats, name) : null;
          const busy = util !== null && util > 0.9;
          return (
            <section key={name} className="card query" aria-labelledby={`q-${name}`}>
              <div className="query-head">
                <div>
                  <h2 id={`q-${name}`}>{about.title}</h2>
                  <div className="mono-label">{about.window}</div>
                </div>
                <span className={`badge ${!q ? "idle" : busy ? "warn" : "ok"}`}>{!q ? "Starting" : busy ? "Busy" : "Healthy"}</span>
              </div>
              <p className="caption">{about.purpose}</p>

              <div className="query-metric">
                <div className="kpi-value">{q ? fmtInt(q.processed_rows_per_second) : "—"}</div>
                <div className="mono-label">rows / second capacity</div>
              </div>

              <div className="meter" aria-label="Capacity used">
                <div className="meter-track">
                  <div className={`meter-fill${busy ? " warn" : ""}`} style={{ width: `${Math.min(100, (util ?? 0) * 100)}%` }} />
                </div>
                <div className="meter-label">
                  {util === null ? "Waiting for the first batch" : `Using ${Math.round(util * 100)}% of capacity`}
                </div>
              </div>

              <div className="outputs">
                <div className="mono-label">
                  Produces {about.outputs.length} features{about.suffix && ` · suffix ${about.suffix}`}
                </div>
                <ul className="output-tags">
                  {about.outputs.map((o) => (
                    <li key={o}>{o}</li>
                  ))}
                </ul>
              </div>

              <dl className="rows">
                <dt>Incoming</dt>
                <dd>{q ? `${fmt1(q.input_rows_per_second)} rows/s` : "—"}</dd>
                <dt>Last batch</dt>
                <dd>{q ? `${fmtInt(q.num_input_rows)} rows in ${fmtMs(q.trigger_execution_ms)}` : "—"}</dd>
                <dt>State kept in memory</dt>
                <dd>{q ? (q.state_rows ? `${fmtInt(q.state_rows)} windows` : "none (stateless)") : "—"}</dd>
                <dt>Batches completed</dt>
                <dd>{q ? fmtInt(q.batch_id + 1) : "—"}</dd>
                <dt>Updated</dt>
                <dd>{fmtAgo(age)}</dd>
              </dl>
            </section>
          );
        })}
      </div>

      <p className="footnote">
        Capacity used = incoming rows ÷ rows Spark can process per second. Under 100% the query keeps up; above it,
        batches start to queue. Late events are accepted up to the 2-minute watermark.
      </p>
    </div>
  );
}

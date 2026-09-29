import type { Stats } from "../types";
import { batchAge, QUERY_NAMES } from "../hooks";
import { fmtCompact, fmtInt, fmtMs } from "../format";

type Tone = "ok" | "warn" | "idle";

interface Stage {
  name: string;
  role: string;
  metric: string;
  tone: Tone;
}

/** The whole system as five stages, each with a live status: a mental model at a glance. */
export function PipelineFlow({ stats, offline }: { stats: Stats | null; offline: boolean }) {
  const reporting = stats ? QUERY_NAMES.filter((n) => stats.queries[n]).length : 0;
  const fresh = stats ? QUERY_NAMES.map((n) => batchAge(stats, n)).filter((a): a is number => a !== null) : [];
  const flowing = !!stats && stats.events_per_minute > 0 && fresh.length > 0 && Math.max(...fresh) < 60_000;
  const batchMs = stats?.queries.window_5m?.trigger_execution_ms ?? stats?.queries.profile?.trigger_execution_ms;
  const t = (ok: boolean): Tone => (offline ? "warn" : ok ? "ok" : "idle");

  const stages: Stage[] = [
    { name: "Producer", role: "Validates events against the contract", metric: stats ? `${fmtInt(stats.events_per_minute / 60)} events/s` : "—", tone: t(flowing) },
    { name: "Kafka", role: "Durable, partitioned event log", metric: "user-events · 6 partitions", tone: t(flowing) },
    { name: "Spark", role: "Sliding-window aggregation", metric: `${reporting}/3 queries${batchMs ? ` · ${fmtMs(batchMs)} batches` : ""}`, tone: t(reporting === 3 && flowing) },
    { name: "Redis", role: "Online store", metric: stats ? `${fmtCompact(stats.online_store_keys)} users` : "—", tone: t(!!stats && stats.online_store_keys > 0) },
    { name: "Feature API", role: "Serves features to models", metric: stats?.serving.p50_ms != null ? `p50 ${fmtMs(stats.serving.p50_ms)}` : "ready", tone: t(!!stats) },
  ];

  return (
    <ol className="flow" aria-label="Data flow">
      {stages.map((s, i) => (
        <li key={s.name} className="flow-stage">
          <span className={`dot ${s.tone}`} aria-hidden="true" />
          <div className="flow-text">
            <div className="flow-name">
              {s.name}
              <span className="flow-metric">{s.metric}</span>
            </div>
            <div className="flow-role">{s.role}</div>
          </div>
          {i < stages.length - 1 && <span className="flow-connector" aria-hidden="true" />}
        </li>
      ))}
    </ol>
  );
}

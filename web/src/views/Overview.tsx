import { Kpi } from "../components/Kpi";
import { PipelineFlow } from "../components/PipelineFlow";
import { Sparkline } from "../components/Sparkline";
import { QUERY_NAMES, batchAge } from "../hooks";
import type { Health, Sample, Stats } from "../types";
import { fmtCompact, fmtInt, fmtMs } from "../format";

interface Props {
  stats: Stats | null;
  health: Health;
  history: Sample[];
  now: number;
}

export function Overview({ stats, health, history, now }: Props) {
  const reporting = stats ? QUERY_NAMES.filter((n) => stats.queries[n]).length : 0;
  const stale = stats ? QUERY_NAMES.some((n) => (batchAge(stats, n) ?? 0) > 60_000) : false;
  const peak = history.length ? Math.max(...history.map((s) => s.epm)) : 0;

  return (
    <div className="view overview">
      <header className="view-head">
        <p className="eyebrow">Overview</p>
        <h1>The stream, right now.</h1>
        <p className="lead">{health.detail}</p>
      </header>

      <div className="kpis">
        <Kpi
          label="Events per minute"
          value={stats ? fmtCompact(stats.events_per_minute) : "—"}
          caption={stats ? `About ${fmtInt(stats.events_per_minute / 60)} user events arrive every second.` : "Waiting for data."}
        />
        <Kpi
          label="Users with live features"
          value={stats ? fmtInt(stats.online_store_keys) : "—"}
          caption="Each has an up-to-date feature vector in Redis, ready to serve."
        />
        <Kpi
          label="Lookup latency · p50"
          value={stats?.serving.p50_ms != null ? fmtMs(stats.serving.p50_ms) : "—"}
          caption={
            stats?.serving.samples
              ? `Median over the last ${fmtInt(stats.serving.samples)} lookups (p95 ${fmtMs(stats.serving.p95_ms)}).`
              : "Look up a user in Features to start measuring."
          }
        />
        <Kpi
          label="Pipeline health"
          value={`${reporting} / 3`}
          unit="queries"
          tone={reporting === 3 && !stale ? "good" : "warn"}
          caption={reporting === 3 && !stale ? "All streaming queries are processing batches." : "Some queries are not reporting yet."}
        />
      </div>

      <div className="overview-grid">
        <section className="card chart-card" aria-labelledby="tp-title">
          <div className="card-head">
            <div>
              <div className="mono-label" id="tp-title">
                Throughput · live
              </div>
              <p className="caption">Events per minute read by Spark from Kafka.</p>
            </div>
            <div className="mono-label">peak {fmtInt(peak)}</div>
          </div>
          <Sparkline samples={history} now={now} />
        </section>

        <section className="card" aria-labelledby="flow-title">
          <div className="card-head">
            <div>
              <div className="mono-label" id="flow-title">
                How data flows
              </div>
              <p className="caption">From raw event to servable feature, in seconds.</p>
            </div>
          </div>
          <PipelineFlow stats={stats} offline={health.state === "offline"} />
        </section>
      </div>
    </div>
  );
}

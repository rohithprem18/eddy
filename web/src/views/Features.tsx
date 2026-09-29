import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import { useRecent } from "../hooks";
import type { FeatureVector } from "../types";
import { fmt2, fmtInt, fmtMs } from "../format";

// Funnel first, then value, then breadth: the order an analyst reads them in.
const WINDOW_FIELDS: [string, string, "int" | "money" | "ratio"][] = [
  ["event_count", "Events", "int"],
  ["view_count", "Views", "int"],
  ["cart_count", "Added to cart", "int"],
  ["purchase_count", "Purchases", "int"],
  ["cart_conversion", "Cart → purchase", "ratio"],
  ["spend", "Spend", "money"],
  ["avg_order_value", "Avg order value", "money"],
  ["refund_amount", "Refunds", "money"],
  ["distinct_products", "Distinct products", "int"],
  ["distinct_categories", "Distinct categories", "int"],
];

const PROFILE_FIELDS: [string, string][] = [
  ["last_event_type", "Last event"],
  ["last_category", "Last category"],
  ["last_product_id", "Last product"],
  ["last_device", "Last device"],
];

function fmtValue(v: number | string | undefined, kind: "int" | "money" | "ratio"): string {
  if (v === undefined) return "—";
  if (typeof v === "string") return v;
  if (kind === "money") return `$${fmt2(v)}`;
  if (kind === "ratio") return `${Math.round(v * 100)}%`;
  return fmtInt(v);
}

type State =
  | { kind: "idle" }
  | { kind: "loading"; userId: string }
  | { kind: "found"; data: FeatureVector }
  | { kind: "missing"; userId: string }
  | { kind: "error"; message: string };

export function Features() {
  const [query, setQuery] = useState("");
  const [state, setState] = useState<State>({ kind: "idle" });
  const [active, setActive] = useState<string[]>([]);
  const [recent, pushRecent] = useRecent("eddy.recentLookups");
  const [copied, setCopied] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    api.sampleUsers(8).then((r) => setActive(r.user_ids)).catch(() => setActive([]));
    // "/" focuses search from anywhere on this view, as in most dashboards.
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "/" && document.activeElement !== input.current) {
        e.preventDefault();
        input.current?.focus();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const lookup = async (raw: string) => {
    const userId = raw.trim();
    if (!userId) return;
    setQuery(userId);
    setState({ kind: "loading", userId });
    try {
      const data = await api.features(userId);
      setState({ kind: "found", data });
      pushRecent(userId);
    } catch (e) {
      const status = (e as { status?: number }).status;
      setState(status === 404 ? { kind: "missing", userId } : { kind: "error", message: "The feature API did not respond." });
    }
  };

  const shown = state.kind === "found" ? state.data : null;
  const curl = `curl ${window.location.origin}/features/${shown?.user_id ?? "u000000"}`;

  return (
    <div className="view features">
      <header className="view-head">
        <p className="eyebrow">Feature lookup</p>
        <h1>Ask for a user.</h1>
        <p className="lead">The same call a model makes at prediction time: one user id in, a fresh feature vector out.</p>
      </header>

      <form
        className="search"
        onSubmit={(e) => {
          e.preventDefault();
          lookup(query);
        }}
      >
        <label htmlFor="uid" className="visually-hidden">
          User id
        </label>
        <input
          id="uid"
          ref={input}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter a user id, e.g. u000000"
          autoComplete="off"
          spellCheck={false}
        />
        <kbd className="kbd" aria-hidden="true">
          /
        </kbd>
        <button className="pill primary" type="submit" disabled={!query.trim() || state.kind === "loading"}>
          {state.kind === "loading" ? "Looking up…" : "Look up"}
        </button>
      </form>

      <div className="chip-rows">
        {recent.length > 0 && (
          <div className="chip-row">
            <span className="mono-label">Recent</span>
            {recent.map((u) => (
              <button key={u} type="button" className="pill sm" onClick={() => lookup(u)}>
                {u}
              </button>
            ))}
          </div>
        )}
        <div className="chip-row">
          <span className="mono-label">Active now</span>
          {active.length ? (
            active.map((u) => (
              <button key={u} type="button" className="pill sm" onClick={() => lookup(u)}>
                {u}
              </button>
            ))
          ) : (
            <span className="caption">No users yet. Features appear about a minute after the stream starts.</span>
          )}
        </div>
      </div>

      {state.kind === "idle" && (
        <div className="empty card">
          <div className="mono-label">What you'll see</div>
          <p>
            Two sliding windows of behaviour (the last 5 minutes and the last hour) plus a last-seen profile, all
            computed from the live event stream. Pick a user above to start.
          </p>
        </div>
      )}
      {state.kind === "missing" && (
        <div className="empty card">
          <div className="mono-label">No features</div>
          <p>
            <strong>{state.userId}</strong> has no events in the current windows. User ids look like u000123; try one
            of the active users above.
          </p>
        </div>
      )}
      {state.kind === "error" && (
        <div className="empty card warn" role="alert">
          <div className="mono-label">Lookup failed</div>
          <p>{state.message}</p>
        </div>
      )}

      {shown && (
        <>
          <div className="result-meta">
            <span className="mono-label">
              User <strong>{shown.user_id}</strong>
            </span>
            <span className="mono-label">Last event {shown.freshness_ms != null ? `${(shown.freshness_ms / 1000).toFixed(1)}s ago` : "—"}</span>
            <span className="mono-label">Served in {fmtMs(shown.latency_ms)}</span>
            <button
              type="button"
              className="pill sm"
              onClick={() => {
                navigator.clipboard?.writeText(curl).then(() => {
                  setCopied(true);
                  window.setTimeout(() => setCopied(false), 1500);
                });
              }}
            >
              {copied ? "Copied" : "Copy curl"}
            </button>
          </div>
          <div className="result-grid">
            {(["_5m", "_1h"] as const).map((suffix) => (
              <section key={suffix} className="card">
                <div className="mono-label">{suffix === "_5m" ? "Last 5 minutes" : "Last hour"}</div>
                <dl className="rows">
                  {WINDOW_FIELDS.map(([key, label, kind]) => (
                    <FragmentRow key={key} label={label} value={fmtValue(shown.features[key + suffix], kind)} />
                  ))}
                </dl>
              </section>
            ))}
            <section className="card">
              <div className="mono-label">Profile</div>
              <dl className="rows">
                {PROFILE_FIELDS.map(([key, label]) => (
                  <FragmentRow key={key} label={label} value={String(shown.features[key] ?? "—")} />
                ))}
              </dl>
              <div className="code">
                <div className="mono-label">Call it from a model</div>
                <code>{curl}</code>
              </div>
            </section>
          </div>
        </>
      )}
    </div>
  );
}

function FragmentRow({ label, value }: { label: string; value: string }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </>
  );
}

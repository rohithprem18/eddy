import { useEffect, useRef, useState } from "react";
import { api } from "./api";
import type { Health, QueryName, Sample, Stats } from "./types";

export const ROUTES = ["overview", "pipeline", "features", "contracts"] as const;
export type Route = (typeof ROUTES)[number];

function currentRoute(): Route {
  const hash = window.location.hash.replace(/^#\/?/, "").split("?")[0];
  return (ROUTES as readonly string[]).includes(hash) ? (hash as Route) : "overview";
}

/** Hash-based routing: bookmarkable views without a server-side router. */
export function useRoute(): [Route, (r: Route) => void] {
  const [route, setRoute] = useState<Route>(currentRoute);
  useEffect(() => {
    const onHash = () => setRoute(currentRoute());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);
  return [route, (r) => (window.location.hash = `/${r}`)];
}

const HISTORY_POINTS = 100; // 100 samples x 3 s = 5 minutes

/** Polls /stats every 3 s and keeps a short events-per-minute history for charts. */
export function useLiveStats() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [error, setError] = useState(false);
  const [history, setHistory] = useState<Sample[]>([]);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    let controller: AbortController | null = null;
    const tick = async () => {
      controller?.abort();
      controller = new AbortController();
      try {
        const s = await api.stats(controller.signal);
        if (cancelled) return;
        setStats(s);
        setError(false);
        setUpdatedAt(Date.now());
        setHistory((h) => [...h, { t: Date.now(), epm: s.events_per_minute }].slice(-HISTORY_POINTS));
      } catch (e) {
        if (!cancelled && !(e instanceof DOMException && e.name === "AbortError")) setError(true);
      }
    };
    tick();
    const timer = window.setInterval(tick, 3000);
    return () => {
      cancelled = true;
      controller?.abort();
      window.clearInterval(timer);
    };
  }, []);

  return { stats, error, history, updatedAt };
}

export const QUERY_NAMES: QueryName[] = ["window_5m", "window_1h", "profile"];

/** Milliseconds since a query's last completed batch, measured on the server clock. */
export function batchAge(stats: Stats, name: QueryName): number | null {
  const q = stats.queries[name];
  if (!q?.timestamp) return null;
  const t = Date.parse(q.timestamp);
  return Number.isNaN(t) ? null : Math.max(0, stats.server_time_ms - t);
}

/** Turns raw metrics into one plain-language status for the navbar. */
export function deriveHealth(stats: Stats | null, error: boolean): Health {
  if (error) {
    return { state: "offline", label: "Offline", detail: "The feature API is not responding. Is the stack running?" };
  }
  if (!stats) return { state: "starting", label: "Connecting", detail: "Contacting the feature API…" };
  const ages = QUERY_NAMES.map((n) => batchAge(stats, n)).filter((a): a is number => a !== null);
  if (ages.length === 0) {
    return { state: "starting", label: "Starting", detail: "Waiting for Spark's first micro-batch." };
  }
  const oldest = Math.max(...ages);
  if (oldest > 60_000) {
    return {
      state: "stalled",
      label: "Stalled",
      detail: `No new Spark batches for ${Math.round(oldest / 1000)}s. Check the spark and producer containers.`,
    };
  }
  return {
    state: "live",
    label: "Streaming",
    detail: `${ages.length} of 3 streaming queries reporting; last batch ${Math.round(Math.min(...ages) / 1000)}s ago.`,
  };
}

/** Remembers the last few feature lookups (recognition beats recall). */
export function useRecent(key: string, max = 6): [string[], (v: string) => void] {
  const [items, setItems] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem(key) ?? "[]");
    } catch {
      return [];
    }
  });
  const push = (v: string) =>
    setItems((prev) => {
      const next = [v, ...prev.filter((x) => x !== v)].slice(0, max);
      try {
        localStorage.setItem(key, JSON.stringify(next));
      } catch {
        /* storage unavailable */
      }
      return next;
    });
  return [items, push];
}

/** Runs `fn` on mount and every `ms` milliseconds while the view is open. */
export function useInterval(fn: () => void, ms: number) {
  const saved = useRef(fn);
  saved.current = fn;
  useEffect(() => {
    saved.current();
    const id = window.setInterval(() => saved.current(), ms);
    return () => window.clearInterval(id);
  }, [ms]);
}

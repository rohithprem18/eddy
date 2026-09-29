const intFmt = new Intl.NumberFormat("en-US");
const oneDp = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
const twoDp = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });

export const fmtInt = (n: number | null | undefined) => (n == null ? "—" : intFmt.format(Math.round(n)));
export const fmt1 = (n: number | null | undefined) => (n == null ? "—" : oneDp.format(n));
export const fmt2 = (n: number | null | undefined) => (n == null ? "—" : twoDp.format(n));

/** 59,480 → "59.5k" for compact headlines. */
export function fmtCompact(n: number): string {
  if (n >= 1_000_000) return `${oneDp.format(n / 1_000_000)}M`;
  if (n >= 10_000) return `${oneDp.format(n / 1000)}k`;
  return intFmt.format(Math.round(n));
}

export function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1) return `${twoDp.format(ms)} ms`;
  if (ms < 1000) return `${oneDp.format(ms)} ms`;
  return `${oneDp.format(ms / 1000)} s`;
}

export function fmtAgo(ms: number | null | undefined): string {
  if (ms == null) return "—";
  const s = Math.max(0, ms / 1000);
  if (s < 1) return "just now";
  if (s < 60) return `${Math.round(s)}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export function fmtClock(t: number): string {
  return new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

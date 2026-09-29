import type { Route } from "../hooks";
import type { Health, Stats } from "../types";
import { fmtAgo, fmtCompact } from "../format";

const LINKS: { route: Route; label: string }[] = [
  { route: "overview", label: "Overview" },
  { route: "pipeline", label: "Pipeline" },
  { route: "features", label: "Features" },
  { route: "contracts", label: "Contracts" },
];

interface Props {
  route: Route;
  health: Health;
  stats: Stats | null;
  updatedAt: number | null;
  now: number;
}

export function Navbar({ route, health, stats, updatedAt, now }: Props) {
  const headline =
    health.state === "live" && stats ? `${fmtCompact(stats.events_per_minute)} events/min` : health.label;
  return (
    <header className="navbar">
      <div className="nav-inner">
        <a className="brand" href="#/overview" aria-label="Eddy overview">
          Eddy
        </a>

        <nav className="nav-links" aria-label="Sections">
          {LINKS.map((l) => (
            <a key={l.route} href={`#/${l.route}`} className={route === l.route ? "active" : undefined} aria-current={route === l.route ? "page" : undefined}>
              {l.label}
            </a>
          ))}
        </nav>

        <div className="nav-right">
          <div className={`health ${health.state}`} title={health.detail} role="status" aria-live="polite">
            <span className="dot" aria-hidden="true" />
            <span className="health-label">{health.state === "live" ? "Streaming" : health.label}</span>
            {health.state === "live" && <span className="health-metric">{headline}</span>}
            <span className="visually-hidden">
              {health.detail} Updated {fmtAgo(updatedAt ? now - updatedAt : null)}.
            </span>
          </div>
          <a className="pill sm" href="/docs" target="_blank" rel="noopener noreferrer">
            API docs
          </a>
        </div>
      </div>
    </header>
  );
}

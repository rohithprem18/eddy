import type { ReactNode } from "react";

interface Props {
  label: string;
  value: ReactNode;
  unit?: string;
  caption: ReactNode;
  tone?: "default" | "good" | "warn";
}

/** One number, one label, one plain-language sentence explaining it. */
export function Kpi({ label, value, unit, caption, tone = "default" }: Props) {
  return (
    <section className={`card kpi ${tone}`}>
      <div className="mono-label">{label}</div>
      <div className="kpi-value">
        {value}
        {unit && <span className="kpi-unit">{unit}</span>}
      </div>
      <p className="caption">{caption}</p>
    </section>
  );
}

import { useState } from "react";
import { api } from "../api";
import { useInterval } from "../hooks";
import type { Contracts as ContractsData, DeadLetters } from "../types";
import { fmtAgo, fmtInt } from "../format";

function short(error: string | null): string {
  if (!error) return "Unknown error";
  // Serializer errors are long; the first clause says what was wrong.
  return error.replace(/\s+/g, " ").split(" (")[0].slice(0, 160);
}

export function Contracts({ now }: { now: number }) {
  const [contracts, setContracts] = useState<ContractsData | null>(null);
  const [dlq, setDlq] = useState<DeadLetters | null>(null);
  const [selected, setSelected] = useState<number | null>(null);

  useInterval(() => {
    api.contracts().then(setContracts).catch(() => setContracts({ available: false, error: "unreachable" }));
  }, 30_000);
  useInterval(() => {
    api.deadLetters(8).then(setDlq).catch(() => setDlq({ available: false, error: "unreachable" }));
  }, 5_000);

  const versions = contracts?.available ? contracts.versions : [];
  const current = versions.find((v) => v.version === selected) ?? versions[versions.length - 1];

  return (
    <div className="view contracts">
      <header className="view-head">
        <p className="eyebrow">Data contracts</p>
        <h1>What producers must send.</h1>
        <p className="lead">
          Every event is checked against a versioned Avro schema before it reaches Kafka. Events that break the
          contract go to a dead-letter topic instead of breaking the pipeline.
        </p>
      </header>

      <div className="contracts-grid">
        <section className="card" aria-labelledby="schema-title">
          <div className="card-head">
            <div>
              <div className="mono-label" id="schema-title">
                Schema registry
              </div>
              <p className="caption">{contracts?.available ? contracts.subject : "Loading…"}</p>
            </div>
            {contracts?.available && <span className="badge ok">{contracts.compatibility} compatible</span>}
          </div>

          {contracts && !contracts.available && (
            <p className="caption">The schema registry is not reachable right now.</p>
          )}

          {versions.length > 0 && current && (
            <>
              <div className="pill-group" role="tablist" aria-label="Schema versions">
                {versions.map((v) => (
                  <button
                    key={v.version}
                    type="button"
                    role="tab"
                    aria-selected={v.version === current.version}
                    className={`pill sm${v.version === current.version ? " selected" : ""}`}
                    onClick={() => setSelected(v.version)}
                  >
                    v{v.version}
                    {v.version === versions[versions.length - 1].version ? " · latest" : ""}
                  </button>
                ))}
              </div>
              <p className="caption schema-doc">{current.doc}</p>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Field</th>
                      <th>Type</th>
                      <th aria-label="Change" />
                    </tr>
                  </thead>
                  <tbody>
                    {current.fields.map((f) => (
                      <tr key={f.name}>
                        <td className="mono">{f.name}</td>
                        <td className="mono muted">{f.type}</td>
                        <td>{f.added && <span className="tag">new in v{current.version}</span>}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="footnote">
                Consumers decode each event with the exact schema version it was written with, so v1 and v2 events can
                share the topic safely.
              </p>
            </>
          )}
        </section>

        <section className="card" aria-labelledby="dlq-title">
          <div className="card-head">
            <div>
              <div className="mono-label" id="dlq-title">
                Dead-letter queue
              </div>
              <p className="caption">
                {dlq?.available ? `${dlq.topic} · rejections here are expected` : "Loading…"}
              </p>
            </div>
            {dlq?.available && (
              <div className="dlq-count">
                <div className="kpi-value">{fmtInt(dlq.total)}</div>
                <div className="mono-label">rejected events</div>
              </div>
            )}
          </div>

          {dlq && !dlq.available && <p className="caption">Kafka is not reachable right now.</p>}
          {dlq?.available && dlq.recent.length === 0 && (
            <p className="caption">No rejected events. Every event so far has matched the contract.</p>
          )}
          {dlq?.available && dlq.recent.length > 0 && (
            <ul className="dlq-list">
              {dlq.recent.map((d, i) => (
                <li key={i}>
                  <div className="dlq-row">
                    <span className="dlq-error">{short(d.error)}</span>
                    <span className="mono-label">{fmtAgo(d.timestamp_ms ? now - d.timestamp_ms : null)}</span>
                  </div>
                  <code className="dlq-event">{JSON.stringify(d.event)}</code>
                </li>
              ))}
            </ul>
          )}
          <p className="footnote">
            Working as designed: the producer deliberately sends ~0.1% malformed test events (INVALID_EVENT_RATE) to
            prove bad data is caught and set aside without stopping the stream.
          </p>
        </section>
      </div>
    </div>
  );
}

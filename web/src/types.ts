export interface QueryProgress {
  batch_id: number;
  timestamp: string;
  num_input_rows: number;
  input_rows_per_second: number;
  processed_rows_per_second: number;
  trigger_execution_ms: number;
  state_rows: number;
}

export type QueryName = "window_5m" | "window_1h" | "profile";

export interface Stats {
  online_store_keys: number;
  events_per_minute: number;
  queries: Record<QueryName, QueryProgress | null>;
  serving: { samples: number; p50_ms: number | null; p95_ms: number | null };
  server_time_ms: number;
}

export interface FeatureVector {
  user_id: string;
  features: Record<string, number | string>;
  found: boolean;
  freshness_ms: number | null;
  latency_ms: number;
}

export interface SchemaField {
  name: string;
  type: string;
  added: boolean;
}

export interface SchemaVersion {
  version: number;
  id: number;
  doc: string;
  fields: SchemaField[];
  removed: string[];
}

export type Contracts =
  | { available: true; subject: string; compatibility: string; versions: SchemaVersion[] }
  | { available: false; error: string };

export interface DeadLetter {
  error: string | null;
  event: Record<string, unknown> | null;
  timestamp_ms: number | null;
}

export type DeadLetters =
  | { available: true; topic: string; total: number; recent: DeadLetter[] }
  | { available: false; error: string };

export type HealthState = "live" | "starting" | "stalled" | "offline";

export interface Health {
  state: HealthState;
  label: string;
  detail: string;
}

export interface Sample {
  t: number;
  epm: number;
}

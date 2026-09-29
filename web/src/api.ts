import type { Contracts, DeadLetters, FeatureVector, Stats } from "./types";

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { cache: "no-store", signal });
  if (!res.ok) {
    const err = new Error(`${path} returned ${res.status}`) as Error & { status?: number };
    err.status = res.status;
    throw err;
  }
  return res.json() as Promise<T>;
}

export const api = {
  stats: (signal?: AbortSignal) => getJson<Stats>("/stats", signal),
  features: (userId: string, signal?: AbortSignal) =>
    getJson<FeatureVector>(`/features/${encodeURIComponent(userId)}`, signal),
  sampleUsers: (n = 8) => getJson<{ user_ids: string[] }>(`/users/sample?n=${n}`),
  contracts: () => getJson<Contracts>("/contracts"),
  deadLetters: (limit = 8) => getJson<DeadLetters>(`/dlq?limit=${limit}`),
};

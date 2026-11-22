// Regression tests for issue #4: every console view must show skeleton
// placeholders (not empty panels or bare "Loading…" text) while its first
// request is in flight. Dependency-free so CI can run it with plain node.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const src = join(dirname(fileURLToPath(import.meta.url)), "..", "src");
const read = (p) => readFileSync(join(src, p), "utf8");

const VIEWS = ["views/Overview.tsx", "views/Pipeline.tsx", "views/Features.tsx", "views/Contracts.tsx"];

test("styles.css defines the shimmer skeleton and respects reduced motion", () => {
  const css = read("styles.css");
  assert.match(css, /\.skeleton\s*\{[^}]*linear-gradient/s, ".skeleton needs the shimmer gradient");
  assert.match(css, /@keyframes shimmer/, "shimmer keyframes missing");
  assert.match(
    css,
    /prefers-reduced-motion: reduce[\s\S]*\.skeleton\s*\{\s*animation:\s*none/,
    "skeletons must stop animating under prefers-reduced-motion",
  );
});

test("Skeleton component exists, is aria-hidden, and has an accessible loading region", () => {
  const tsx = read("components/Skeleton.tsx");
  assert.match(tsx, /export function Skeleton\(/, "base Skeleton component missing");
  assert.match(tsx, /aria-hidden="true"/, "shimmer blocks must be hidden from screen readers");
  assert.match(tsx, /role="status"/, "a role=status region must announce the loading state");
  assert.match(tsx, /visually-hidden/, "the status label must use the visually-hidden class");
});

for (const view of VIEWS) {
  test(`${view} imports and renders skeletons while loading`, () => {
    const code = read(view);
    assert.match(code, /from "\.\.\/components\/Skeleton"/, `${view} must import from components/Skeleton`);
    assert.match(code, /<\w*Skeleton|SkeletonLines|SkeletonRegion/, `${view} must render a skeleton`);
  });
}

test("Contracts view no longer shows bare Loading… text placeholders", () => {
  const code = read("views/Contracts.tsx");
  assert.ok(!code.includes('"Loading…"'), "raw Loading… text should be replaced by skeletons");
});

test("Features view shows a skeleton result while a lookup is in flight", () => {
  const code = read("views/Features.tsx");
  assert.match(code, /state\.kind === "loading"/, "loading state must exist");
  assert.match(code, /<FeatureResultSkeleton|SkeletonRegion/, "a skeleton result must render while loading");
});

test("Overview shows skeleton KPI cards and cards while stats are null", () => {
  const code = read("views/Overview.tsx");
  assert.match(code, /stats \?/, "Overview must branch on stats being present");
  assert.match(code, /<OverviewSkeleton/, "Overview must render the skeleton layout while stats load");
});

test("every composed skeleton export is used (by a view or by another skeleton)", () => {
  const tsx = read("components/Skeleton.tsx");
  const primitives = new Set(["Skeleton", "SkeletonLines", "SkeletonRegion"]);
  const names = [...tsx.matchAll(/export function (\w+)/g)].map((m) => m[1]);
  assert.ok(names.length >= 6, "expected several composed skeleton components");
  const usage = VIEWS.map(read).join("\n") + "\n" + tsx;
  for (const name of names) {
    if (primitives.has(name)) continue;
    assert.ok(usage.includes(`<${name}`), `${name} is exported but never rendered anywhere`);
  }
});

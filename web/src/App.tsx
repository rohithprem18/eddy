import { useEffect, useState } from "react";
import { Navbar } from "./components/Navbar";
import { deriveHealth, useLiveStats, useRoute } from "./hooks";
import { Contracts } from "./views/Contracts";
import { Features } from "./views/Features";
import { Overview } from "./views/Overview";
import { Pipeline } from "./views/Pipeline";

const TITLES = { overview: "Overview", pipeline: "Pipeline", features: "Features", contracts: "Contracts" } as const;

export default function App() {
  const [route] = useRoute();
  const { stats, error, history, updatedAt } = useLiveStats();
  const [now, setNow] = useState(Date.now());
  const health = deriveHealth(stats, error);

  // A 1 s clock keeps "x seconds ago" labels and the chart moving between polls.
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    document.title = `${TITLES[route]} · Eddy`;
  }, [route]);

  return (
    <div className="shell">
      <Navbar route={route} health={health} stats={stats} updatedAt={updatedAt} now={now} />
      <main className="stage" key={route}>
        {route === "overview" && <Overview stats={stats} health={health} history={history} now={now} />}
        {route === "pipeline" && <Pipeline stats={stats} />}
        {route === "features" && <Features />}
        {route === "contracts" && <Contracts now={now} />}
      </main>
    </div>
  );
}

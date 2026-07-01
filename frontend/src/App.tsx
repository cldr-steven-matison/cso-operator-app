import { useState } from "react";

import { AllTopics } from "@/components/AllTopics";
import { DemoMode } from "@/components/DemoMode";
import { EfmPage } from "@/components/EfmPage";
import { HealthBar } from "@/components/HealthBar";
import { Ingest } from "@/components/Ingest";
import { KafkaActivity } from "@/components/KafkaActivity";
import { NifiControls } from "@/components/NifiControls";
import { Operators } from "@/components/Operators";
import { PodSummary } from "@/components/PodSummary";
import { QdrantPanel } from "@/components/QdrantPanel";
import { RagQuery } from "@/components/RagQuery";
import { StreamersPage } from "@/components/StreamersPage";
import { cn } from "@/lib/utils";

const _modules = (import.meta.env.VITE_MODULES ?? "").split(",").map((s: string) => s.trim());
const _has = (m: string) => _modules.includes(m) || _modules.includes("all");
const _efm = _has("efm");
const _rag = _has("rag");
const _streamers = _has("streamers");

type Tab = "operator" | "efm" | "rag" | "streamers";

// Operator is always present. EFM, RAG, Streamers require the matching MODULES flag.
const TABS: { id: Tab; label: string }[] = [
  ...(_streamers ? [{ id: "streamers" as Tab, label: "Streamers" }] : []),
  ...(_rag ? [{ id: "rag" as Tab, label: "RAG" }] : []),
  { id: "operator", label: "Operator" },
  ...(_efm ? [{ id: "efm" as Tab, label: "EFM" }] : []),
];

export default function App() {
  const [tab, setTab] = useState<Tab>(TABS[0].id);
  return (
    <div className="min-h-full flex flex-col">
      <HealthBar />
      <nav className="flex items-center gap-1 px-4 border-b border-border bg-panel">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={cn(
              "px-3 py-2 text-sm border-b-2 -mb-px transition-colors",
              tab === t.id
                ? "border-accent text-text"
                : "border-transparent text-muted hover:text-text",
            )}
          >
            {t.label}
          </button>
        ))}
      </nav>
      <main className="flex-1 p-4 max-w-[1600px] mx-auto w-full space-y-4">
        {tab === "operator" && (
          <>
            <Operators />
            <PodSummary />
          </>
        )}
        {tab === "efm" && _efm && <EfmPage />}
        {tab === "streamers" && _streamers && <StreamersPage />}
        {tab === "rag" && (
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            <DemoMode />
            <Ingest />
            <NifiControls />
            <KafkaActivity />
            <QdrantPanel />
            <RagQuery />
            <AllTopics />
          </div>
        )}
      </main>
    </div>
  );
}

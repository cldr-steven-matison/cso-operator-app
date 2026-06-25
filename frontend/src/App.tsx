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
import { cn } from "@/lib/utils";

type Tab = "operator" | "efm" | "rag";

const TABS: { id: Tab; label: string }[] = [
  { id: "operator", label: "Operator" },
  { id: "efm", label: "EFM" },
  { id: "rag", label: "RAG" },
];

export default function App() {
  const [tab, setTab] = useState<Tab>("operator");
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
        {tab === "efm" && <EfmPage />}
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

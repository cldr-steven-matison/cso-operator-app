import { AllTopics } from "@/components/AllTopics";
import { DemoMode } from "@/components/DemoMode";
import { HealthBar } from "@/components/HealthBar";
import { Ingest } from "@/components/Ingest";
import { KafkaActivity } from "@/components/KafkaActivity";
import { NifiControls } from "@/components/NifiControls";
import { Operators } from "@/components/Operators";
import { PodSummary } from "@/components/PodSummary";
import { QdrantPanel } from "@/components/QdrantPanel";
import { RagQuery } from "@/components/RagQuery";

export default function App() {
  return (
    <div className="min-h-full flex flex-col">
      <HealthBar />
      <main className="flex-1 p-4 max-w-[1600px] mx-auto w-full space-y-4">
        <Operators />
        <PodSummary />
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <DemoMode />
          <Ingest />
          <NifiControls />
          <KafkaActivity />
          <QdrantPanel />
          <RagQuery />
          <AllTopics />
        </div>
      </main>
    </div>
  );
}

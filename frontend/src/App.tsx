import { AllTopics } from "@/components/AllTopics";
import { DemoMode } from "@/components/DemoMode";
import { HealthBar } from "@/components/HealthBar";
import { Ingest } from "@/components/Ingest";
import { KafkaActivity } from "@/components/KafkaActivity";
import { NifiControls } from "@/components/NifiControls";
import { QdrantPanel } from "@/components/QdrantPanel";
import { RagQuery } from "@/components/RagQuery";

export default function App() {
  return (
    <div className="min-h-full flex flex-col">
      <HealthBar />
      <main className="flex-1 p-4 grid grid-cols-1 xl:grid-cols-2 gap-4 max-w-[1600px] mx-auto w-full">
        <DemoMode />
        <Ingest />
        <NifiControls />
        <KafkaActivity />
        <QdrantPanel />
        <RagQuery />
        <AllTopics />
      </main>
    </div>
  );
}

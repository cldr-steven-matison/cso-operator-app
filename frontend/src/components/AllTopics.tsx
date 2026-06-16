import { useEffect, useState } from "react";

import { Card, CardTitle } from "@/components/ui/Card";
import { api, type KafkaAllTopic, type KafkaAllTopicsResponse } from "@/lib/api";

function asArray(r: KafkaAllTopicsResponse): KafkaAllTopic[] {
  return Array.isArray(r) ? r : r.topics;
}

const HIGHLIGHT = new Set(["new_audio", "new_documents"]);

export function AllTopics() {
  const [topics, setTopics] = useState<KafkaAllTopic[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const r = await api.kafkaAllTopics();
        if (!alive) return;
        setTopics(asArray(r));
        setError(Array.isArray(r) ? null : r.error);
      } catch (e) {
        if (alive) setError(String(e));
      }
    };
    refresh();
    const id = setInterval(refresh, 10000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  return (
    <Card className="col-span-1 xl:col-span-2">
      <CardTitle>All Kafka Topics ({topics.length})</CardTitle>
      {error && <div className="text-bad text-xs mb-2">{error}</div>}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2 text-xs">
        {topics.map((t) => (
          <div
            key={t.topic}
            className={`border rounded p-2 ${
              HIGHLIGHT.has(t.topic)
                ? "border-accent/40 bg-accent/5"
                : "border-border bg-bg"
            }`}
          >
            <div className="truncate" title={t.topic}>
              {t.topic}
            </div>
            <div className="text-muted">
              {t.partitions}p · depth {t.depth}
            </div>
          </div>
        ))}
        {topics.length === 0 && !error && (
          <div className="text-muted col-span-full">no topics</div>
        )}
      </div>
    </Card>
  );
}

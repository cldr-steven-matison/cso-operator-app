import { useEffect, useRef, useState } from "react";

import { Card, CardTitle } from "@/components/ui/Card";
import { TopicPeek } from "@/components/TopicPeek";
import { api, openSSE, type KafkaTopic } from "@/lib/api";

function asArray(r: KafkaTopic[] | { error: string; topics: KafkaTopic[] }): KafkaTopic[] {
  return Array.isArray(r) ? r : r.topics;
}

type Msg = { topic: string; partition: number; offset: number; ts: number; size: number; payload: string };

const TOPICS = ["new_audio", "new_documents"];

export function KafkaActivity() {
  const [topics, setTopics] = useState<KafkaTopic[]>([]);
  const [messages, setMessages] = useState<Record<string, Msg[]>>({});
  const [peekOpen, setPeekOpen] = useState<Record<string, boolean>>({});
  const tailers = useRef<(() => void)[]>([]);

  useEffect(() => {
    const refresh = async () => {
      try {
        setTopics(asArray(await api.kafkaTopics()));
      } catch {}
    };
    refresh();
    const id = setInterval(refresh, 5000);

    tailers.current = TOPICS.map((t) =>
      openSSE(`/api/kafka/tail/${t}`, (_evt, data) => {
        try {
          const m = JSON.parse(data) as Msg;
          setMessages((prev) => {
            const arr = prev[t] ?? [];
            return { ...prev, [t]: [m, ...arr].slice(0, 25) };
          });
        } catch {}
      })
    );

    return () => {
      clearInterval(id);
      tailers.current.forEach((c) => c());
    };
  }, []);

  return (
    <Card>
      <CardTitle>Kafka Activity</CardTitle>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {TOPICS.map((t) => {
          const stat = topics.find((x) => x.topic === t);
          const msgs = messages[t] ?? [];
          const isOpen = !!peekOpen[t];
          return (
            <div key={t} className="border border-border rounded p-3 bg-bg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">{t}</span>
                <span className="text-xs text-muted">
                  {stat?.exists ? `${stat.partitions}p · depth ${stat.depth}` : "—"}
                </span>
              </div>
              <div className="text-xs space-y-1 max-h-48 overflow-y-auto">
                {msgs.length === 0 && <div className="text-muted">no messages yet</div>}
                {msgs.map((m) => (
                  <div key={`${m.partition}:${m.offset}`} className="text-text">
                    <span className="text-muted">[p{m.partition}/o{m.offset}]</span>{" "}
                    <span className="text-muted">{m.size}b</span>{" "}
                    {t === "new_audio" ? (
                      <span className="text-muted italic">binary</span>
                    ) : (
                      <span>{m.payload.slice(0, 80)}</span>
                    )}
                  </div>
                ))}
              </div>
              <button
                className="mt-2 text-xs text-muted hover:text-text"
                onClick={() => setPeekOpen((o) => ({ ...o, [t]: !o[t] }))}
              >
                {isOpen ? "hide last 10" : "peek last 10"}
              </button>
              {isOpen && <TopicPeek topic={t} />}
            </div>
          );
        })}
      </div>
    </Card>
  );
}

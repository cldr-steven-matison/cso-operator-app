import { useEffect, useRef, useState } from "react";

import { api, type KafkaPeekMsg } from "@/lib/api";

function formatTs(ts: number | null): string {
  if (!ts) return "";
  try {
    return new Date(ts).toISOString().slice(11, 19);
  } catch {
    return "";
  }
}

/**
 * Inline last-N preview for any Kafka topic. Mounted by AllTopics under the
 * tile the user clicked, and by KafkaActivity under each watched topic.
 *
 * Polls every 5s while mounted; the parent controls visibility.
 */
export function TopicPeek({ topic, limit = 10 }: { topic: string; limit?: number }) {
  const [msgs, setMsgs] = useState<KafkaPeekMsg[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const reqId = useRef(0);

  const refresh = async () => {
    const id = ++reqId.current;
    setLoading(true);
    try {
      const data = await api.kafkaPeek(topic, limit);
      if (id !== reqId.current) return;
      setMsgs(data);
      setError(null);
    } catch (e) {
      if (id !== reqId.current) return;
      setError(String(e));
    } finally {
      if (id === reqId.current) setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
    const i = setInterval(refresh, 5000);
    return () => {
      reqId.current++;
      clearInterval(i);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topic, limit]);

  return (
    <div className="mt-2 border-t border-border pt-2 text-xs">
      <div className="flex items-center justify-between mb-1">
        <span className="text-muted">last {limit} {loading ? "· loading…" : ""}</span>
        <button
          className="text-muted hover:text-text"
          onClick={(e) => {
            e.stopPropagation();
            refresh();
          }}
        >
          refresh
        </button>
      </div>
      {error && <div className="text-bad">{error}</div>}
      {msgs && msgs.length === 0 && !error && (
        <div className="text-muted italic">no messages</div>
      )}
      <div className="space-y-1 max-h-64 overflow-y-auto">
        {msgs?.map((m) => (
          <div key={`${m.partition}:${m.offset}`} className="leading-snug">
            <div className="text-muted">
              [p{m.partition}/o{m.offset}] {formatTs(m.ts)} · {m.size}b
            </div>
            <div className="break-all">
              {m.payload_b64 ? (
                <span className="italic text-muted">
                  &lt;{m.size} bytes binary&gt;
                </span>
              ) : (
                m.payload || <span className="italic text-muted">empty</span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

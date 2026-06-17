import { useState } from "react";

import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { openSSE } from "@/lib/api";

type Source = { id: string; score: number; payload: { text?: string; source?: string } };

export function RagQuery() {
  const [q, setQ] = useState("What is StreamToVLLM?");
  const [answer, setAnswer] = useState("");
  const [sources, setSources] = useState<Source[]>([]);
  const [streaming, setStreaming] = useState(false);
  const [showSources, setShowSources] = useState(false);
  const [error, setError] = useState<string>("");

  const ask = async () => {
    if (!q.trim() || streaming) return;
    setAnswer("");
    setSources([]);
    setError("");
    setStreaming(true);

    const close = openSSE(
      "/api/query",
      (event, data) => {
        if (event === "sources") {
          try {
            setSources(JSON.parse(data));
          } catch {}
          return;
        }
        if (event === "error") {
          // Backend surfaces vLLM / qdrant / embedding failures here.
          try {
            const obj = JSON.parse(data);
            setError(obj.error + (obj.body ? `\n${obj.body}` : ""));
          } catch {
            setError(data);
          }
          return;
        }
        if (data === "[DONE]") {
          setStreaming(false);
          close();
          return;
        }
        try {
          const obj = JSON.parse(data);
          const delta = obj?.choices?.[0]?.delta?.content;
          if (delta) setAnswer((a) => a + delta);
        } catch {}
      },
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q }),
      }
    );
  };

  return (
    <Card>
      <CardTitle>RAG Query</CardTitle>
      <div className="flex gap-2 mb-3">
        <input
          className="flex-1 px-3 py-2 rounded bg-bg border border-border text-sm"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && ask()}
          placeholder="Ask a question..."
        />
        <Button onClick={ask} disabled={streaming}>
          {streaming ? "Streaming..." : "Ask"}
        </Button>
      </div>

      <div className="border border-border rounded p-3 bg-bg min-h-[120px] whitespace-pre-wrap text-sm">
        {answer || <span className="text-muted">answer will stream here</span>}
      </div>
      {error && (
        <div className="mt-2 border border-red-500/40 bg-red-500/10 text-red-300 rounded p-2 text-xs whitespace-pre-wrap break-all">
          {error}
        </div>
      )}

      {sources.length > 0 && (
        <div className="mt-2">
          <button
            className="text-xs text-muted hover:text-text"
            onClick={() => setShowSources((s) => !s)}
          >
            {showSources ? "▾" : "▸"} sources ({sources.length})
          </button>
          {showSources && (
            <div className="mt-2 space-y-2">
              {sources.map((s) => (
                <div key={s.id} className="border border-border rounded p-2 bg-bg text-xs">
                  <div className="text-muted mb-1">
                    score {s.score?.toFixed?.(3)}
                    {s.payload?.source && ` · ${s.payload.source}`}
                  </div>
                  <div className="whitespace-pre-wrap">
                    {(s.payload?.text ?? "").slice(0, 500)}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

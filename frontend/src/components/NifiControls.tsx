import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type NifiState } from "@/lib/api";

const FLOWS = ["IngestDocsToStream", "IngestDataToStream", "StreamToWhisper", "StreamTovLLM"] as const;

function tone(state?: string) {
  if (!state) return "neutral" as const;
  const s = state.toUpperCase();
  if (s === "RUNNING") return "ok" as const;
  if (s === "STOPPED") return "warn" as const;
  if (s === "INVALID") return "bad" as const;
  return "neutral" as const;
}

type Optimistic = "starting" | "stopping";

export function NifiControls() {
  const [state, setState] = useState<NifiState>({});
  const [busy, setBusy] = useState<string | null>(null);
  const [optimistic, setOptimistic] = useState<Record<string, Optimistic>>({});

  const refresh = async () => {
    try {
      setState(await api.nifiState());
    } catch {
      // surfaced via health bar
    }
  };

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 4000);
    return () => clearInterval(id);
  }, []);

  const act = async (name: string, op: Optimistic, fn: () => Promise<unknown>) => {
    setBusy(name);
    setOptimistic((o) => ({ ...o, [name]: op }));
    try {
      await fn();
      // small delay so NiFi finishes propagating the start/stop, then refresh
      await new Promise((r) => setTimeout(r, 300));
      await refresh();
    } finally {
      setBusy(null);
      setOptimistic((o) => {
        const next = { ...o };
        delete next[name];
        return next;
      });
    }
  };

  return (
    <Card>
      <CardTitle>NiFi Flows</CardTitle>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {FLOWS.map((name) => {
          const pg = state[name];
          const opt = optimistic[name];
          const displayState = opt
            ? opt === "starting"
              ? "STARTING…"
              : "STOPPING…"
            : pg?.state ?? "not deployed";
          const displayTone = opt ? "warn" : pg ? tone(pg.state) : "neutral";
          return (
            <div key={name} className="border border-border rounded p-3 bg-bg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">{name}</span>
                <Badge tone={displayTone}>{displayState}</Badge>
              </div>
              <div className="flex gap-2">
                <Button
                  disabled={!pg || busy === name}
                  onClick={() => act(name, "starting", () => api.nifiStart(name))}
                >
                  Start
                </Button>
                <Button
                  variant="ghost"
                  disabled={!pg || busy === name}
                  onClick={() => act(name, "stopping", () => api.nifiStop(name))}
                >
                  Stop
                </Button>
              </div>
            </div>
          );
        })}
      </div>
    </Card>
  );
}

import { useEffect, useState } from "react";

import { Dot } from "@/components/ui/Badge";
import { api, type Health } from "@/lib/api";
import { cn } from "@/lib/utils";

const ORDER: (keyof Health["services"])[] = ["vllm", "embedding", "qdrant", "whisper", "nifi", "kafka"];

export function HealthBar() {
  const [h, setH] = useState<Health | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const data = await api.health();
        if (alive) {
          setH(data);
          setErr(null);
        }
      } catch (e) {
        if (alive) setErr(String(e));
      }
    };
    tick();
    const id = setInterval(tick, 5000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  return (
    <div className="flex items-center gap-4 px-4 py-2 border-b border-border bg-panel">
      <span className="font-bold tracking-wide">CSO Operator App</span>
      <div className="flex items-center gap-3 ml-auto text-xs">
        {ORDER.map((k) => {
          const s = h?.services[k];
          const tone = !s ? "neutral" : s.ok ? "ok" : "bad";
          const label = `${k}${s && !s.ok && s.error ? `: ${s.error.slice(0, 40)}` : ""}`;
          return (
            <span key={k} className={cn("flex items-center gap-1.5")} title={label}>
              <Dot tone={tone} />
              <span className="text-muted">{k}</span>
            </span>
          );
        })}
        {err && <span className="text-bad text-xs ml-2">offline</span>}
      </div>
    </div>
  );
}

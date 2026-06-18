import { useEffect, useState } from "react";

import { Dot } from "@/components/ui/Badge";
import { api, type Health, type Operator } from "@/lib/api";
import { cn } from "@/lib/utils";

const ORDER: (keyof Health["services"])[] = ["vllm", "embedding", "qdrant", "whisper", "nifi", "kafka"];

export function HealthBar() {
  const [h, setH] = useState<Health | null>(null);
  const [ops, setOps] = useState<Operator[] | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const [data, opsData] = await Promise.all([api.health(), api.k8sOperators()]);
        if (alive) {
          setH(data);
          setOps(opsData);
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

  // CSA operator status — green when the Flink operator deployment is ready.
  // Lives next to the backing-service dots even though it comes from a
  // different endpoint (/api/k8s/operators).
  const flink = ops?.find((o) => o.name.startsWith("CSA"));
  const flinkTone = !flink
    ? "neutral"
    : flink.installed && flink.replicas > 0 && flink.ready === flink.replicas
      ? "ok"
      : flink.installed
        ? "warn"
        : "bad";
  const flinkLabel = flink
    ? `flink: ${flink.installed ? `${flink.ready}/${flink.replicas}` : "absent"}`
    : "flink";

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
        <span className="flex items-center gap-1.5" title={flinkLabel}>
          <Dot tone={flinkTone} />
          <span className="text-muted">flink</span>
        </span>
        {err && <span className="text-bad text-xs ml-2">offline</span>}
      </div>
    </div>
  );
}

import { useEffect, useState } from "react";

import { Badge, Dot } from "@/components/ui/Badge";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type Operator } from "@/lib/api";

function tone(op: Operator): "ok" | "warn" | "bad" {
  if (!op.installed) return "bad";
  if (op.replicas > 0 && op.ready === op.replicas) return "ok";
  return "warn";
}

export function Operators() {
  const [ops, setOps] = useState<Operator[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const r = await api.k8sOperators();
        if (!alive) return;
        setOps(r);
        setError(null);
      } catch (e) {
        if (alive) setError(String(e));
      }
    };
    refresh();
    let id = setInterval(refresh, 60000);
    const onVisibility = () => {
      if (document.hidden) {
        clearInterval(id);
      } else {
        refresh();
        id = setInterval(refresh, 60000);
      }
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      alive = false;
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  return (
    <Card>
      <CardTitle>Cloudera Operators</CardTitle>
      {error && <div className="text-bad text-xs mb-2">{error}</div>}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {ops.map((op) => (
          <div
            key={op.deployment}
            className="border border-border rounded p-3 bg-bg"
          >
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-2">
                <Dot tone={tone(op)} />
                <span className="text-sm">{op.name}</span>
              </div>
              <Badge tone={tone(op)}>
                {op.installed ? `${op.ready}/${op.replicas}` : "absent"}
              </Badge>
            </div>
            <div className="text-xs text-muted space-y-0.5">
              <div>
                {op.namespace}/{op.deployment}
                {op.version ? ` · ${op.version}` : ""}
              </div>
              <div className="truncate" title={op.image}>
                {op.image || "—"}
              </div>
              <div>
                CRDs: {op.crds_present} ({op.crd_groups.join(", ")})
              </div>
              {op.error && <div className="text-bad">{op.error}</div>}
            </div>
          </div>
        ))}
        {ops.length === 0 && !error && (
          <div className="text-muted text-xs">loading…</div>
        )}
      </div>
    </Card>
  );
}

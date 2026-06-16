import { useEffect, useState } from "react";

import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type QdrantStats } from "@/lib/api";

export function QdrantPanel() {
  const [stats, setStats] = useState<QdrantStats | null>(null);
  const [busy, setBusy] = useState(false);

  const refresh = async () => {
    try {
      setStats(await api.qdrantStats());
    } catch {}
  };

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  const recreate = async () => {
    if (!confirm("Drop and recreate my-rag-collection?")) return;
    setBusy(true);
    try {
      await api.qdrantRecreate();
      await refresh();
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card>
      <CardTitle>Qdrant — my-rag-collection</CardTitle>
      <div className="grid grid-cols-3 gap-3 text-sm mb-3">
        <Stat label="exists" value={String(stats?.exists ?? "?")} />
        <Stat label="points" value={String(stats?.points_count ?? 0)} />
        <Stat label="status" value={stats?.status ?? "?"} />
      </div>
      <Button variant="danger" onClick={recreate} disabled={busy}>
        {busy ? "Recreating..." : "Recreate Collection"}
      </Button>
    </Card>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-border rounded p-2 bg-bg">
      <div className="text-muted text-xs">{label}</div>
      <div className="text-text">{value}</div>
    </div>
  );
}

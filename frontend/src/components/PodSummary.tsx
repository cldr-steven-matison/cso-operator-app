import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type PodInfo, type PodSummary as PodSummaryNs } from "@/lib/api";

function phaseTone(phase: string): "ok" | "warn" | "bad" | "neutral" {
  if (phase === "Running") return "ok";
  if (phase === "Pending") return "warn";
  if (phase === "Failed") return "bad";
  if (phase === "Succeeded") return "neutral";
  return "neutral";
}

function age(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

/**
 * `kubectl rollout restart` operates on the deployment, but we only have the
 * pod's owner reference (a ReplicaSet). The RS name is `<deploy>-<hash>`, so
 * stripping the trailing `-<10-alnum>` segment recovers the deployment name
 * 99% of the time. Returns null when the owner isn't a ReplicaSet.
 */
function deploymentForPod(p: PodInfo): string | null {
  if (p.owner_kind !== "ReplicaSet" || !p.owner_name) return null;
  const m = p.owner_name.match(/^(.+)-[a-z0-9]{8,12}$/);
  return m ? m[1] : p.owner_name;
}

export function PodSummary() {
  const [data, setData] = useState<PodSummaryNs[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const r = await api.k8sPods();
      setData(r);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 5000);
    return () => clearInterval(id);
  }, []);

  const restart = async (ns: string, deployment: string) => {
    const key = `restart:${ns}/${deployment}`;
    setBusy(key);
    try {
      await api.k8sRestart(ns, deployment);
      await new Promise((r) => setTimeout(r, 400));
      await refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(null);
    }
  };

  const deletePod = async (ns: string, name: string) => {
    const key = `delete:${ns}/${name}`;
    setBusy(key);
    try {
      await api.k8sDeletePod(ns, name);
      setConfirmDelete(null);
      await new Promise((r) => setTimeout(r, 400));
      await refresh();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(null);
    }
  };

  return (
    <Card>
      <CardTitle>Pods</CardTitle>
      {error && <div className="text-bad text-xs mb-2">{error}</div>}
      <div className="space-y-3">
        {data.map((nsData) => (
          <div key={nsData.ns} className="border border-border rounded">
            <div className="flex items-center justify-between px-3 py-2 bg-bg/40 border-b border-border">
              <span className="text-sm">{nsData.ns}</span>
              <span className="text-xs text-muted">
                {nsData.total} total · {nsData.running} running
                {nsData.pending ? ` · ${nsData.pending} pending` : ""}
                {nsData.failed ? ` · ${nsData.failed} failed` : ""}
              </span>
            </div>
            {nsData.error && (
              <div className="text-bad text-xs px-3 py-2">{nsData.error}</div>
            )}
            <div className="divide-y divide-border text-xs">
              {nsData.pods.map((p) => {
                const deploy = deploymentForPod(p);
                const podKey = `${nsData.ns}/${p.name}`;
                const restartKey = deploy ? `restart:${nsData.ns}/${deploy}` : "";
                const deleteKey = `delete:${podKey}`;
                return (
                  <div
                    key={p.name}
                    className="flex items-center justify-between gap-2 px-3 py-1.5"
                  >
                    <div className="min-w-0 flex-1">
                      <div className="truncate font-mono" title={p.name}>
                        {p.name}
                      </div>
                      <div className="text-muted">
                        ready {p.ready}/{p.containers} · restarts {p.restarts} ·{" "}
                        {age(p.age_seconds)} · {p.node || "?"}
                      </div>
                    </div>
                    <Badge tone={phaseTone(p.phase)}>{p.phase}</Badge>
                    {deploy && (
                      <Button
                        variant="ghost"
                        className="text-xs px-2 py-0.5"
                        title={`rollout restart deploy/${deploy}`}
                        disabled={busy === restartKey}
                        onClick={() => restart(nsData.ns, deploy)}
                      >
                        {busy === restartKey ? "…" : "restart"}
                      </Button>
                    )}
                    {confirmDelete === podKey ? (
                      <span className="flex gap-1">
                        <Button
                          variant="danger"
                          className="text-xs px-2 py-0.5"
                          disabled={busy === deleteKey}
                          onClick={() => deletePod(nsData.ns, p.name)}
                        >
                          yes
                        </Button>
                        <Button
                          variant="ghost"
                          className="text-xs px-2 py-0.5"
                          onClick={() => setConfirmDelete(null)}
                        >
                          no
                        </Button>
                      </span>
                    ) : (
                      <Button
                        variant="ghost"
                        className="text-xs px-2 py-0.5"
                        onClick={() => setConfirmDelete(podKey)}
                      >
                        delete
                      </Button>
                    )}
                  </div>
                );
              })}
              {nsData.pods.length === 0 && !nsData.error && (
                <div className="px-3 py-2 text-muted">no pods</div>
              )}
            </div>
          </div>
        ))}
        {data.length === 0 && !error && (
          <div className="text-muted text-xs">loading…</div>
        )}
      </div>
    </Card>
  );
}

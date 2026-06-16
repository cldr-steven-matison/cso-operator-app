import { useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api } from "@/lib/api";

const FLOWS = ["IngestDocsToStream", "IngestDataToStream", "StreamToWhisper", "StreamTovLLM"] as const;

export function DemoMode() {
  const [step, setStep] = useState(0);
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const append = (s: string) => setLog((l) => [...l, s]);

  const startAll = async () => {
    setBusy(true);
    append("Starting all flows...");
    try {
      for (const f of FLOWS) {
        await api.nifiStart(f);
        append(`  ✓ ${f}`);
      }
      setStep(1);
    } catch (e) {
      append(`  ! ${String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card>
      <CardTitle>Demo Mode</CardTitle>
      <ol className="space-y-2 text-sm">
        <Step n={1} active={step === 0} done={step > 0}>
          Start all four flows.
          <div className="mt-1">
            <Button onClick={startAll} disabled={busy || step > 0}>
              {step > 0 ? "Started" : "Start flows"}
            </Button>
          </div>
        </Step>
        <Step n={2} active={step === 1} done={step > 1}>
          Drop a doc or audio file in the <em>Ingest</em> panel — or click <em>Use sample audio</em>.
        </Step>
        <Step n={3} active={step === 2} done={step > 2}>
          Watch <em>Kafka Activity</em> — <code>new_audio</code> for audio,{" "}
          <code>new_documents</code> for docs and transcripts.
        </Step>
        <Step n={4} active={step === 3} done={false}>
          In <em>RAG Query</em>, ask <em>What is StreamToVLLM?</em> (docs) or{" "}
          <em>How is rice prepared?</em> (sample audio).
        </Step>
      </ol>

      {log.length > 0 && (
        <div className="mt-3 text-xs text-muted whitespace-pre-wrap">
          {log.join("\n")}
        </div>
      )}

      <div className="mt-3 flex gap-2">
        <Button variant="ghost" onClick={() => setStep((s) => Math.min(3, s + 1))}>
          Next step
        </Button>
        <Button
          variant="ghost"
          onClick={() => {
            setStep(0);
            setLog([]);
          }}
        >
          Reset
        </Button>
      </div>
    </Card>
  );
}

function Step({
  n,
  active,
  done,
  children,
}: {
  n: number;
  active: boolean;
  done: boolean;
  children: React.ReactNode;
}) {
  return (
    <li className="flex gap-3">
      <Badge tone={done ? "ok" : active ? "warn" : "neutral"}>{n}</Badge>
      <div className={active ? "" : "text-muted"}>{children}</div>
    </li>
  );
}

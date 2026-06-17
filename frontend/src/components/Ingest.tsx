import { useState } from "react";

import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api } from "@/lib/api";

type IngestResult = {
  url?: string;
  status?: number;
  ok?: boolean;
  response?: string;
  content_type?: string;
  filename?: string;
  bytes?: number;
};

export function Ingest() {
  const [status, setStatus] = useState<string>("");
  const [busy, setBusy] = useState(false);

  const onFile = async (file: File) => {
    setBusy(true);
    setStatus(`Uploading ${file.name} (${file.type || "?"})...`);
    try {
      const res: IngestResult = await api.ingest(file);
      const verdict = res.ok ? "✓" : "✗";
      setStatus(
        `${verdict} NiFi ${res.status} · ${res.filename} · ${res.content_type} · ${res.bytes}B${
          res.response ? ` · ${res.response.slice(0, 120)}` : ""
        }`
      );
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  const useSample = async () => {
    setBusy(true);
    setStatus("Fetching sample WAV through backend proxy...");
    try {
      const r = await fetch(api.sampleAudioUrl);
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      const blob = await r.blob();
      const file = new File([blob], "OSR_us_000_0010_8k.wav", { type: "audio/wav" });
      await onFile(file);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
      setBusy(false);
    }
  };

  return (
    <Card>
      <CardTitle>Ingest</CardTitle>
      <label
        className={`block border-2 border-dashed border-border rounded p-6 text-center cursor-pointer hover:border-muted ${busy ? "opacity-50" : ""}`}
        onDragOver={(e) => e.preventDefault()}
        onDrop={(e) => {
          e.preventDefault();
          const f = e.dataTransfer.files[0];
          if (f) onFile(f);
        }}
      >
        <input
          type="file"
          className="hidden"
          disabled={busy}
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) onFile(f);
          }}
        />
        <div className="text-muted text-sm">Drop a doc or audio file, or click to choose</div>
        <div className="text-muted text-xs mt-1">
          NiFi RouteOnAttribute branches by mime type
        </div>
      </label>
      {status && <div className="text-xs text-muted mt-2 break-all">{status}</div>}
      <div className="mt-3 flex gap-2">
        <Button variant="ghost" disabled={busy} onClick={useSample}>
          Use sample audio
        </Button>
      </div>
    </Card>
  );
}

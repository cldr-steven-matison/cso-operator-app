import { useState } from "react";

import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api } from "@/lib/api";

const AUDIO_EXT = [".wav", ".mp3", ".m4a", ".flac", ".ogg"];

function isAudio(name: string) {
  const lower = name.toLowerCase();
  return AUDIO_EXT.some((e) => lower.endsWith(e));
}

export function Ingest() {
  const [status, setStatus] = useState<string>("");
  const [busy, setBusy] = useState(false);

  const onFile = async (file: File) => {
    setBusy(true);
    setStatus(`Uploading ${file.name}...`);
    try {
      const audio = isAudio(file.name);
      const res = audio ? await api.ingestAudio(file) : await api.ingestDoc(file);
      setStatus(`Sent → ${audio ? "IngestDataToStream" : "IngestToStream"} (${res.status}, ${res.bytes} bytes)`);
    } catch (e) {
      setStatus(`Error: ${String(e)}`);
    } finally {
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
        <div className="text-muted text-xs mt-1">audio → IngestDataToStream · other → IngestToStream</div>
      </label>
      {status && <div className="text-xs text-muted mt-2">{status}</div>}
      <div className="mt-3 flex gap-2">
        <Button
          variant="ghost"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            setStatus("Fetching sample WAV...");
            try {
              const r = await fetch(
                "https://www.voiptroubleshooter.com/open_speech/american/OSR_us_000_0010_8k.wav"
              );
              const blob = await r.blob();
              const file = new File([blob], "OSR_us_000_0010_8k.wav", { type: "audio/wav" });
              await onFile(file);
            } catch (e) {
              setStatus(`Error: ${String(e)}`);
              setBusy(false);
            }
          }}
        >
          Use sample audio
        </Button>
      </div>
    </Card>
  );
}

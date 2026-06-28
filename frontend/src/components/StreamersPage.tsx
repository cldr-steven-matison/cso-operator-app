import { useEffect, useRef, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type StreamerClip, type StreamerFlows } from "@/lib/api";

// ── helpers ────────────────────────────────────────────────────────────────

function stateColor(state: string): string {
  if (state === "RUNNING") return "text-accent";
  if (state === "STOPPED") return "text-muted";
  if (state === "NOT_INSTALLED") return "text-bad";
  return "text-warn";
}

function stateTone(state: string): "ok" | "bad" | "warn" | "neutral" {
  if (state === "RUNNING") return "ok";
  if (state === "NOT_INSTALLED") return "bad";
  if (state === "STOPPED") return "neutral";
  return "warn";
}

// ── FlowCard ───────────────────────────────────────────────────────────────

function FlowCard({
  name,
  state,
  onStart,
  onStop,
}: {
  name: string;
  state: string;
  onStart: () => void;
  onStop: () => void;
}) {
  const [busy, setBusy] = useState(false);

  async function toggle() {
    setBusy(true);
    try {
      if (state === "RUNNING") await onStop();
      else await onStart();
    } finally {
      setBusy(false);
    }
  }

  const notInstalled = state === "NOT_INSTALLED";

  return (
    <div className="border border-border rounded p-4 bg-bg flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="font-mono text-sm text-text">{name}</span>
        <Badge tone={stateTone(state)}>{state}</Badge>
      </div>
      <Button
        onClick={toggle}
        disabled={busy || notInstalled}
        className="text-xs"
      >
        {busy
          ? state === "RUNNING"
            ? "Stopping…"
            : "Starting…"
          : notInstalled
          ? "Not Installed"
          : state === "RUNNING"
          ? "Stop"
          : "Start"}
      </Button>
    </div>
  );
}

// ── ClipCard ───────────────────────────────────────────────────────────────

function ClipCard({ clip, onPublished }: { clip: StreamerClip; onPublished: () => void }) {
  const [caption, setCaption] = useState(clip.caption ?? "");
  const [commentary, setCommentary] = useState("");
  const [publishing, setPublishing] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; url?: string; error?: string } | null>(null);
  const [transcriptOpen, setTranscriptOpen] = useState(false);

  const tweetText = [caption, commentary].filter(Boolean).join("\n\n");

  async function doPublish() {
    if (!clip.clip_path || !tweetText.trim()) return;
    setPublishing(true);
    setResult(null);
    try {
      const r = await api.streamersPublish(clip.clip_path, tweetText);
      setResult({ ok: true, url: r.url });
      onPublished();
    } catch (e) {
      setResult({ ok: false, error: String(e) });
    } finally {
      setPublishing(false);
    }
  }

  return (
    <div className="border border-border rounded p-4 bg-bg space-y-3">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="text-sm font-semibold text-text">{clip.title ?? "Untitled Clip"}</p>
          <p className="text-xs text-muted">
            {clip.streamer ?? "Unknown"} · {clip.duration ? `${Math.round(clip.duration)}s` : "—"}
          </p>
        </div>
        {clip.thumbnail_url && (
          <img
            src={clip.thumbnail_url}
            alt="thumbnail"
            className="w-24 h-14 object-cover rounded border border-border shrink-0"
          />
        )}
      </div>

      {/* Transcript toggle */}
      {clip.transcript && (
        <div>
          <button
            onClick={() => setTranscriptOpen((o) => !o)}
            className="text-xs text-muted underline"
          >
            {transcriptOpen ? "Hide transcript" : "Show transcript"}
          </button>
          {transcriptOpen && (
            <p className="mt-1 text-xs font-mono text-muted border border-border rounded p-2 bg-panel max-h-24 overflow-y-auto">
              {clip.transcript}
            </p>
          )}
        </div>
      )}

      {/* Caption (editable) */}
      <div className="space-y-1">
        <label className="text-xs text-muted">Caption (vLLM generated — edit freely)</label>
        <textarea
          rows={2}
          value={caption}
          onChange={(e) => setCaption(e.target.value)}
          className="w-full bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text resize-y"
        />
      </div>

      {/* Commentary */}
      <div className="space-y-1">
        <label className="text-xs text-muted">Your commentary</label>
        <textarea
          rows={2}
          value={commentary}
          onChange={(e) => setCommentary(e.target.value)}
          placeholder="Add your take…"
          className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-text resize-y"
        />
      </div>

      {/* Tweet preview */}
      {tweetText.trim() && (
        <div className="border border-border rounded p-2 bg-panel">
          <p className="text-xs text-muted mb-1">Tweet preview ({tweetText.length}/280)</p>
          <p className="text-xs text-text whitespace-pre-wrap">{tweetText}</p>
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-3 flex-wrap">
        <Button
          onClick={doPublish}
          disabled={publishing || !tweetText.trim() || !clip.clip_path}
        >
          {publishing ? "Publishing…" : "Approve & Publish"}
        </Button>
        {result && (
          <span className={result.ok ? "text-accent text-xs" : "text-bad text-xs"}>
            {result.ok ? (
              <a href={result.url} target="_blank" rel="noreferrer" className="underline">
                Posted — view on X
              </a>
            ) : (
              result.error
            )}
          </span>
        )}
      </div>
    </div>
  );
}

// ── WatchList ──────────────────────────────────────────────────────────────

function WatchList() {
  const [logins, setLogins] = useState<string[]>([]);
  const [input, setInput] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.streamersWatchlist().then((r) => setLogins(r.logins)).catch(() => {});
  }, []);

  async function add() {
    const login = input.trim().toLowerCase();
    if (!login || logins.includes(login)) return;
    const next = [...logins, login];
    setInput("");
    setSaving(true);
    try {
      const r = await api.streamersSetWatchlist(next);
      setLogins(r.logins);
    } finally {
      setSaving(false);
    }
  }

  async function remove(login: string) {
    const next = logins.filter((l) => l !== login);
    setSaving(true);
    try {
      const r = await api.streamersSetWatchlist(next);
      setLogins(r.logins);
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card>
      <CardTitle>Watch List</CardTitle>
      <div className="space-y-3">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && add()}
            placeholder="Twitch login (e.g. xqc)"
            className="flex-1 bg-bg border border-border rounded px-2 py-1 text-sm text-text font-mono"
          />
          <Button onClick={add} disabled={saving || !input.trim()}>
            Add
          </Button>
        </div>
        {logins.length === 0 ? (
          <p className="text-xs text-muted">No streamers in watch list</p>
        ) : (
          <div className="flex flex-wrap gap-2">
            {logins.map((login) => (
              <div
                key={login}
                className="flex items-center gap-1 border border-border rounded px-2 py-1 bg-panel text-xs font-mono"
              >
                <span className="text-text">{login}</span>
                <button
                  onClick={() => remove(login)}
                  className="text-muted hover:text-bad ml-1"
                  aria-label={`Remove ${login}`}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}

// ── StreamersPage ──────────────────────────────────────────────────────────

export function StreamersPage() {
  const [flows, setFlows] = useState<StreamerFlows>({});
  const [clips, setClips] = useState<StreamerClip[]>([]);
  const [clipsLoading, setClipsLoading] = useState(true);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refreshFlows = async () => {
    try {
      const f = await api.streamersFlows();
      setFlows(f);
    } catch {}
  };

  const refreshQueue = async () => {
    try {
      const q = await api.streamersQueue();
      setClips(q);
    } catch {} finally {
      setClipsLoading(false);
    }
  };

  useEffect(() => {
    refreshFlows();
    refreshQueue();
    pollRef.current = setInterval(refreshFlows, 5000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const flowNames = ["FetchClips", "ProcessClips", "PublishClip"] as const;

  return (
    <div className="space-y-4">
      {/* ── Section 1: Pipeline Status ─────────────────────────────── */}
      <Card>
        <CardTitle>Pipeline Status</CardTitle>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {flowNames.map((name) => {
            const flow = flows[name] ?? { state: "UNKNOWN" };
            return (
              <FlowCard
                key={name}
                name={name}
                state={flow.state}
                onStart={async () => {
                  await api.streamersFlowStart(name);
                  await refreshFlows();
                }}
                onStop={async () => {
                  await api.streamersFlowStop(name);
                  await refreshFlows();
                }}
              />
            );
          })}
        </div>
      </Card>

      {/* ── Section 2: Clip Review Queue ───────────────────────────── */}
      <Card>
        <div className="flex items-center justify-between mb-2">
          <CardTitle>Clip Review Queue</CardTitle>
          <Button className="text-xs" onClick={refreshQueue}>
            Refresh
          </Button>
        </div>
        {clipsLoading ? (
          <p className="text-muted text-sm">Loading queue…</p>
        ) : clips.length === 0 ? (
          <p className="text-muted text-sm">
            No clips in queue. Start FetchClips and ProcessClips to populate.
          </p>
        ) : (
          <div className="space-y-4">
            {clips.map((clip, i) => (
              <ClipCard
                key={clip._offset ?? i}
                clip={clip}
                onPublished={refreshQueue}
              />
            ))}
          </div>
        )}
      </Card>

      {/* ── Section 3: Watch List ──────────────────────────────────── */}
      <WatchList />
    </div>
  );
}

import { useEffect, useRef, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type StreamerClip, type StreamerFlows, type StreamerTopics } from "@/lib/api";

// ── helpers ────────────────────────────────────────────────────────────────

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

const FALLBACK_CAPTIONS = [
  "Testing the newest twitch content with commentary from Tuna Street 🚀🐟🧑‍🚀",
  "Caught something wild on Twitch — Tuna Street has the take 🐟🔥",
  "Fresh off the stream, straight to your feed — Tuna Street approved 🧑‍🚀🐟",
  "Tuna Street is watching so you don't have to 👀🐟🚀",
  "Another clip, another banger — Tuna Street on the case 🐟💥",
];

function fallbackCaption() {
  return FALLBACK_CAPTIONS[Math.floor(Math.random() * FALLBACK_CAPTIONS.length)];
}

function ClipCard({
  clip,
  onPublished,
  onSkip,
}: {
  clip: StreamerClip;
  onPublished: (offset: number) => void;
  onSkip: (offset: number) => void;
}) {
  const [caption, setCaption] = useState(clip.caption?.trim() || fallbackCaption());
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
      const r = await api.streamersPublish(clip.clip_path, tweetText, clip.clip_id);
      setResult({ ok: true, url: r.url });
      setTimeout(() => onPublished(clip._offset ?? -1), 1200);
    } catch (e) {
      setResult({ ok: false, error: String(e) });
    } finally {
      setPublishing(false);
    }
  }

  async function doSkip() {
    if (clip.clip_id) {
      try { await api.streamersSkip(clip.clip_id); } catch {}
    }
    onSkip(clip._offset ?? -1);
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
            loading="lazy"
            className="w-24 h-14 object-cover rounded border border-border shrink-0"
          />
        )}
      </div>

      {/* Video player */}
      {clip.clip_id && (
        <video
          controls
          preload="none"
          className="w-full rounded border border-border max-h-72"
          src={`/api/streamers/clip/${clip.clip_id}`}
        />
      )}

      {/* Transcript toggle */}
      <div>
        <button
          onClick={() => setTranscriptOpen((o) => !o)}
          className="text-xs text-muted underline"
        >
          {transcriptOpen ? "Hide transcript" : "Show transcript"}
        </button>
        {transcriptOpen && (
          <p className="mt-1 text-xs font-mono text-muted border border-border rounded p-2 bg-panel max-h-24 overflow-y-auto">
            {clip.transcript?.trim() || "No transcript — Whisper may have timed out or returned empty."}
          </p>
        )}
      </div>

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
        <Button
          onClick={doSkip}
          disabled={publishing}
          className="text-xs opacity-60"
        >
          Skip
        </Button>
        {result && (
          <span className={result.ok ? "text-accent text-xs" : "text-bad text-xs"}>
            {result.ok ? (
              <a href={result.url} target="_blank" rel="noreferrer" className="underline">
                Posted — view on X ✓
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

// ── TopicPanel ─────────────────────────────────────────────────────────────

function TopicPanel({ label, stats }: { label: string; stats?: StreamerTopics["new_clips"] }) {
  if (!stats) return (
    <div className="border border-border rounded p-3 text-xs text-muted">Loading {label}…</div>
  );
  return (
    <div className="border border-border rounded p-3 space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-text">{label}</span>
        <span className="text-xs text-muted">{stats.count} message{stats.count !== 1 ? "s" : ""}</span>
      </div>
      {stats.error && <p className="text-xs text-bad">{stats.error}</p>}
      {stats.records.length === 0 ? (
        <p className="text-xs text-muted italic">empty</p>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-muted border-b border-border">
              <th className="text-left py-1 pr-2">off</th>
              <th className="text-left py-1 pr-2">streamer</th>
              <th className="text-left py-1 pr-2">title</th>
              <th className="text-left py-1">file</th>
            </tr>
          </thead>
          <tbody>
            {stats.records.map((r) => (
              <tr key={r.offset} className="border-b border-border last:border-0">
                <td className="py-1 pr-2 text-muted">{r.offset}</td>
                <td className="py-1 pr-2">{r.streamer || "—"}</td>
                <td className="py-1 pr-2 truncate max-w-[180px]">{r.title || r.clip_id || "—"}</td>
                <td className="py-1">{r.has_file ? "✓" : <span className="text-bad">✗</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
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
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());
  const [topics, setTopics] = useState<StreamerTopics | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [resetResult, setResetResult] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refreshFlows = async () => {
    try {
      const f = await api.streamersFlows();
      setFlows(f);
    } catch {}
  };

  const refreshQueue = async () => {
    setDismissed(new Set());
    try {
      const q = await api.streamersQueue();
      setClips(q);
    } catch {} finally {
      setClipsLoading(false);
    }
  };

  const refreshTopics = async () => {
    setTopicsLoading(true);
    try {
      setTopics(await api.streamersTopics());
    } catch {} finally {
      setTopicsLoading(false);
    }
  };

  const doReset = async () => {
    if (!confirm("Wipe both Kafka topics and all downloaded clips?")) return;
    setResetting(true);
    setResetResult(null);
    try {
      const r = await api.streamersReset();
      const errs = r.errors?.length ? ` Errors: ${r.errors.join(", ")}` : "";
      setResetResult(`Deleted: ${r.deleted_topics.join(", ")} | Clips removed: ${r.removed_clips}${errs} — waiting for Kafka…`);
      // Give Strimzi ~4s to actually remove the topics before querying
      await new Promise((res) => setTimeout(res, 4000));
      await refreshQueue();
      await refreshTopics();
      setResetResult(`Done — topics cleared, ${r.removed_clips} clips removed.`);
    } catch (e) {
      setResetResult(`Error: ${String(e)}`);
    } finally {
      setResetting(false);
    }
  };

  const dismiss = (offset: number) =>
    setDismissed((prev) => new Set(prev).add(offset));

  useEffect(() => {
    refreshFlows();
    refreshQueue();
    // topics are expensive (Kafka consumer lifecycle) — manual Refresh only

    const startPoll = () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(() => {
        if (!document.hidden) refreshFlows();
      }, 30000);
    };

    const onVisibility = () => {
      if (document.hidden) {
        if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      } else {
        refreshFlows();
        startPoll();
      }
    };

    document.addEventListener("visibilitychange", onVisibility);
    startPoll();

    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  const flowNames = ["FetchClips", "ProcessClips", "PublishClip"] as const;
  const visibleClips = clips.filter((c) => !dismissed.has(c._offset ?? -1));

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

      {/* ── Section 2: Kafka Topics ────────────────────────────────── */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <CardTitle>Kafka Topics</CardTitle>
          <div className="flex items-center gap-2">
            <Button className="text-xs" onClick={refreshTopics} disabled={topicsLoading}>
              {topicsLoading ? "Loading…" : "Refresh"}
            </Button>
            <Button
              className="text-xs bg-bad text-white hover:opacity-80"
              onClick={doReset}
              disabled={resetting}
            >
              {resetting ? "Resetting…" : "Reset Kafka"}
            </Button>
          </div>
        </div>
        {resetResult && (
          <p className={`text-xs mb-3 ${resetResult.startsWith("Error") ? "text-bad" : "text-accent"}`}>
            {resetResult}
          </p>
        )}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <TopicPanel label="new_clips" stats={topics?.new_clips} />
          <TopicPanel label="processed_clips" stats={topics?.processed_clips} />
        </div>
      </Card>

      {/* ── Section 3: Clip Review Queue ───────────────────────────── */}
      <Card>
        <div className="flex items-center justify-between mb-2">
          <CardTitle>
            Clip Review Queue
            {visibleClips.length > 0 && (
              <span className="ml-2 text-xs text-muted font-normal">
                {visibleClips.length} pending
              </span>
            )}
          </CardTitle>
          <Button className="text-xs" onClick={refreshQueue}>
            Refresh
          </Button>
        </div>
        {clipsLoading ? (
          <p className="text-muted text-sm">Loading queue…</p>
        ) : visibleClips.length === 0 ? (
          <p className="text-muted text-sm">
            {clips.length > 0
              ? "All clips published or skipped."
              : "No clips in queue. Start FetchClips and ProcessClips to populate."}
          </p>
        ) : (
          <div className="space-y-4">
            {visibleClips.map((clip, i) => (
              <ClipCard
                key={clip._offset ?? i}
                clip={clip}
                onPublished={dismiss}
                onSkip={dismiss}
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

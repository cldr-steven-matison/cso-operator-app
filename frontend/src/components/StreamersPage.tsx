import { useEffect, useRef, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import { api, type PendingClip, type StreamerClip, type StreamerFlows, type StreamerTopics } from "@/lib/api";
import { TopicPeek } from "./TopicPeek";

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
  const [publishing, setPublishing] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; position?: number; error?: string } | null>(null);
  const [transcriptOpen, setTranscriptOpen] = useState(false);

  const tweetText = caption;

  async function doPublish() {
    if (!clip.clip_path || !tweetText.trim()) return;
    setPublishing(true);
    setResult(null);
    try {
      const r = await api.streamersApprove(clip.clip_path, tweetText, clip.clip_id, clip.title);
      setResult({ ok: true, position: r.position });
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
        <div className="space-y-1 min-w-0">
          {clip.url ? (
            <a href={clip.url} target="_blank" rel="noopener noreferrer"
               className="text-sm font-semibold text-text hover:text-accent block truncate">
              {clip.title ?? "Untitled Clip"}
            </a>
          ) : (
            <p className="text-sm font-semibold text-text truncate">{clip.title ?? "Untitled Clip"}</p>
          )}
          <div className="flex items-center gap-1.5 flex-wrap text-xs text-muted">
            <PlatformBadge platform={(clip.source ?? "twitch") as "twitch" | "kick"} />
            <a
              href={clip.source === "kick" ? `https://kick.com/${clip.streamer}` : `https://www.twitch.tv/${clip.streamer}`}
              target="_blank" rel="noopener noreferrer"
              className="text-text hover:text-accent font-mono"
            >
              {clip.streamer ?? "Unknown"}
            </a>
            {clip.x_handle && (
              <a href={`https://x.com/${clip.x_handle}`} target="_blank" rel="noopener noreferrer"
                 className="text-accent hover:underline">
                @{clip.x_handle}
              </a>
            )}
            {clip.duration && <span>· {Math.round(clip.duration)}s</span>}
            {clip.view_count != null && clip.view_count > 0 && (
              <span>· {clip.view_count.toLocaleString()} views</span>
            )}
            {clip.created_at && (
              <span>· {new Date(clip.created_at).toLocaleDateString()}</span>
            )}
          </div>
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
        <textarea
          rows={4}
          value={caption}
          onChange={(e) => setCaption(e.target.value)}
          className="w-full bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text resize-y"
        />
      </div>

      {/* Tweet preview */}
      {tweetText.trim() && (
        <div className="border border-border rounded p-2 bg-panel">
          <div className="flex items-center gap-2 mb-1">
            <p className="text-xs text-muted">Tweet preview ({tweetText.length}/280)</p>
            {clip.source && <PlatformBadge platform={clip.source as "twitch" | "kick"} />}
          </div>
          <p className="text-xs text-text whitespace-pre-wrap">{tweetText}</p>
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-3 flex-wrap">
        <Button
          onClick={doPublish}
          disabled={publishing || !tweetText.trim() || !clip.clip_path}
        >
          {publishing ? "Queuing…" : "Approve"}
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
            {result.ok
              ? `Queued #${result.position} ✓`
              : result.error}
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
              <th className="text-left py-1 pr-2">src</th>
              <th className="text-left py-1 pr-2">streamer</th>
              <th className="text-left py-1 pr-2">title</th>
              <th className="text-left py-1">file</th>
            </tr>
          </thead>
          <tbody>
            {stats.records.map((r) => (
              <tr key={r.offset} className="border-b border-border last:border-0">
                <td className="py-1 pr-2 text-muted">{r.offset}</td>
                <td className="py-1 pr-2"><PlatformBadge platform={(r.source ?? "twitch") as "twitch" | "kick"} /></td>
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

// ── PendingPanel ───────────────────────────────────────────────────────────

function PendingPanel({
  pending,
  loading,
  onCancel,
}: {
  pending: PendingClip[];
  loading: boolean;
  onCancel: (clip_id: string) => void;
}) {
  const [cancelingId, setCancelingId] = useState<string | null>(null);

  async function doCancel(clip_id: string) {
    setCancelingId(clip_id);
    try {
      await api.streamersCancelPending(clip_id);
      onCancel(clip_id);
    } finally {
      setCancelingId(null);
    }
  }

  if (loading) return <p className="text-muted text-sm">Loading pending publish queue…</p>;
  if (pending.length === 0) return <p className="text-muted text-sm">Queue empty — nothing waiting to post.</p>;

  return (
    <div className="space-y-2">
      {pending.map((p, i) => (
        <div
          key={p.clip_id || i}
          className="flex items-start justify-between gap-3 border border-border rounded p-3 bg-bg"
        >
          <div className="min-w-0 space-y-1">
            <div className="flex items-center gap-2 text-xs text-muted">
              <span className="font-semibold text-text">#{i + 1}</span>
              <span className="font-mono truncate">{p.clip_id || "unknown clip"}</span>
            </div>
            <p className="text-xs text-text whitespace-pre-wrap line-clamp-2">{p.tweet_text}</p>
          </div>
          <Button
            onClick={() => doCancel(p.clip_id)}
            disabled={cancelingId === p.clip_id}
            className="text-xs opacity-60 shrink-0"
          >
            {cancelingId === p.clip_id ? "Canceling…" : "Cancel"}
          </Button>
        </div>
      ))}
    </div>
  );
}

// ── WatchList ──────────────────────────────────────────────────────────────

function PlatformBadge({ platform }: { platform: "twitch" | "kick" }) {
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide ${
      platform === "kick" ? "bg-green-800 text-green-200" : "bg-purple-900 text-purple-200"
    }`}>
      {platform}
    </span>
  );
}

function WatchList() {
  const [logins, setLogins] = useState<string[]>([]);
  const [input, setInput] = useState("");
  const [platform, setPlatform] = useState<"twitch" | "kick">("twitch");
  const [saving, setSaving] = useState(false);
  const [rotating, setRotating] = useState(false);
  const [fetchMode, setFetchMode] = useState<{ mode: string; period: string }>({ mode: "recent", period: "month" });

  useEffect(() => {
    api.streamersWatchlist().then((r) => setLogins(r.logins)).catch(() => {});
    api.streamersFetchMode().then(setFetchMode).catch(() => {});
  }, []);

  async function updateFetchMode(mode: string, period: string) {
    const updated = await api.streamersSetFetchMode(mode, period);
    setFetchMode(updated);
  }

  async function add() {
    const bare = input.trim().toLowerCase();
    if (!bare) return;
    const login = platform === "kick" ? `kick:${bare}` : bare;
    if (logins.includes(login)) return;
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

  async function rotate() {
    setRotating(true);
    try {
      const r = await api.streamersRotateWatchlist();
      setLogins(r.logins);
    } finally {
      setRotating(false);
    }
  }

  return (
    <Card>
      <div className="flex items-center justify-between mb-3">
        <CardTitle className="mb-0">Watch List</CardTitle>
        <Button onClick={rotate} disabled={rotating}>
          {rotating ? "Rotating…" : "Rotate"}
        </Button>
      </div>
      <div className="space-y-3">
        <div className="flex gap-2">
          <div className="flex rounded border border-border overflow-hidden shrink-0 text-xs font-semibold">
            <button
              onClick={() => setPlatform("twitch")}
              className={`px-2 py-1 uppercase tracking-wide transition-colors ${
                platform === "twitch"
                  ? "bg-purple-900 text-purple-200"
                  : "bg-bg text-muted hover:text-text"
              }`}
            >
              Twitch
            </button>
            <button
              onClick={() => setPlatform("kick")}
              className={`px-2 py-1 uppercase tracking-wide transition-colors border-l border-border ${
                platform === "kick"
                  ? "bg-green-800 text-green-200"
                  : "bg-bg text-muted hover:text-text"
              }`}
            >
              Kick
            </button>
          </div>
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && add()}
            placeholder={platform === "kick" ? "Kick slug (e.g. xqc)" : "Twitch login (e.g. xqc)"}
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
            {logins.map((login) => {
              const isKick = login.startsWith("kick:");
              const displayName = isKick ? login.slice(5) : login;
              return (
                <div
                  key={login}
                  className="flex items-center gap-1.5 border border-border rounded px-2 py-1 bg-panel text-xs font-mono"
                >
                  <PlatformBadge platform={isKick ? "kick" : "twitch"} />
                  <span className="text-text">{displayName}</span>
                  <button
                    onClick={() => remove(login)}
                    className="text-muted hover:text-bad ml-1"
                    aria-label={`Remove ${login}`}
                  >
                    ×
                  </button>
                </div>
              );
            })}
          </div>
        )}
        <div className="pt-2 border-t border-border flex items-center gap-3 flex-wrap">
          <span className="text-xs text-muted">Twitch Fetch Mode:</span>
          <div className="flex rounded border border-border overflow-hidden text-xs font-semibold">
            <button
              onClick={() => updateFetchMode("recent", fetchMode.period)}
              className={`px-2 py-1 transition-colors ${fetchMode.mode === "recent" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
            >
              Recent
            </button>
            <button
              onClick={() => updateFetchMode("top", fetchMode.period)}
              className={`px-2 py-1 border-l border-border transition-colors ${fetchMode.mode === "top" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
            >
              Top Clips
            </button>
          </div>
          {fetchMode.mode === "top" && (
            <div className="flex rounded border border-border overflow-hidden text-xs font-semibold">
              <button
                onClick={() => updateFetchMode("top", "month")}
                className={`px-2 py-1 transition-colors ${fetchMode.period === "month" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
              >
                1 Month
              </button>
              <button
                onClick={() => updateFetchMode("top", "all")}
                className={`px-2 py-1 border-l border-border transition-colors ${fetchMode.period === "all" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
              >
                All Time
              </button>
            </div>
          )}
        </div>
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
  const [peekOpen, setPeekOpen] = useState<Record<string, boolean>>({});
  const [resetting, setResetting] = useState(false);
  const [resetResult, setResetResult] = useState<string | null>(null);
  const [pending, setPending] = useState<PendingClip[]>([]);
  const [pendingLoading, setPendingLoading] = useState(true);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const refreshFlows = async () => {
    try {
      const f = await api.streamersFlows();
      setFlows(f);
    } catch {}
  };

  const refreshPending = async () => {
    try {
      const r = await api.streamersPending();
      setPending(r.pending);
    } catch {} finally {
      setPendingLoading(false);
    }
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

  const onApproved = (offset: number) => {
    dismiss(offset);
    refreshPending();
  };

  const cancelPending = (clip_id: string) =>
    setPending((prev) => prev.filter((p) => p.clip_id !== clip_id));

  useEffect(() => {
    refreshFlows();
    refreshQueue();
    refreshTopics();
    refreshPending();

    const startPoll = () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(() => {
        if (!document.hidden) {
          refreshFlows();
          refreshPending();
        }
      }, 30000);
    };

    const onVisibility = () => {
      if (document.hidden) {
        if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      } else {
        refreshFlows();
        refreshPending();
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

      {/* ── Section 2: Watch List ──────────────────────────────────── */}
      <WatchList />

      {/* ── Section 3: Kafka Topics ────────────────────────────────── */}
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
          {(["new_clips", "processed_clips"] as const).map((t) => (
            <div key={t}>
              <TopicPanel label={t} stats={topics?.[t]} />
              <div
                className={`mt-2 border rounded p-2 cursor-pointer text-xs border-accent/40 bg-accent/5 ${peekOpen[t] ? "ring-1 ring-accent/60" : ""}`}
                onClick={() => setPeekOpen((o) => ({ ...o, [t]: !o[t] }))}
              >
                <span className="font-mono font-semibold text-text">{peekOpen[t] ? "▾ " : "▸ "}{t} payload</span>
                {peekOpen[t] && <TopicPeek topic={t} limit={10} />}
              </div>
            </div>
          ))}
        </div>
      </Card>

      {/* ── Section 4: Clip Review Queue ───────────────────────────── */}
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
                onPublished={onApproved}
                onSkip={dismiss}
              />
            ))}
          </div>
        )}
      </Card>

      {/* ── Section 5: Pending Publish ──────────────────────────────── */}
      <Card>
        <div className="flex items-center justify-between mb-2">
          <CardTitle>
            Pending Publish
            {pending.length > 0 && (
              <span className="ml-2 text-xs text-muted font-normal">
                {pending.length} queued
              </span>
            )}
          </CardTitle>
          <Button className="text-xs" onClick={refreshPending}>
            Refresh
          </Button>
        </div>
        <PendingPanel pending={pending} loading={pendingLoading} onCancel={cancelPending} />
      </Card>

    </div>
  );
}

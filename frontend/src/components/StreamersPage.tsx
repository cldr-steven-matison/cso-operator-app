import { useEffect, useRef, useState } from "react";

import { Badge } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import {
  api,
  openSSE,
  type ChatActivitySnapshot,
  type ChatInspectResult,
  type InspectorChatter,
  type InspectorClip,
  type InspectorResult,
  type PendingClip,
  type PostedClip,
  type RosterPatch,
  type RosterRow,
  type StreamerClip,
  type StreamerFlows,
  type StreamerGif,
  type StreamerTopics,
} from "@/lib/api";
import { TopicPeek } from "./TopicPeek";

// ── helpers ────────────────────────────────────────────────────────────────

// Posting paths (#280). Mirrors approve_clip's fan-out: clip=Y queues the MP4,
// gif_post=Y also queues "{clip_id}-gif"; a clip=N streamer queues only the GIF.
type Paths = { clip: boolean; gif: boolean; gif_post?: boolean };

function approveLabel(p: Paths): string {
  if (p.clip && p.gif_post) return "Clip + GIF";
  if (!p.clip && p.gif_post) return "GIF only";
  if (p.clip) return "Clip";
  return "nothing — no path on";
}

function PathPill({ on, label, title }: { on: boolean; label: string; title: string }) {
  return (
    <span
      title={title}
      className={`px-1.5 py-0.5 rounded border ${on ? "bg-accent/20 text-accent border-accent/40" : "bg-bg text-muted border-border line-through"}`}
    >
      {label}
    </span>
  );
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
  onPostNow,
  onSkip,
}: {
  clip: StreamerClip;
  onPublished: (clipId: string) => void;
  onPostNow: (clipId: string) => void;
  onSkip: (clipId: string) => void;
}) {
  const [caption, setCaption] = useState(clip.caption?.trim() || fallbackCaption());
  const [publishing, setPublishing] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; position?: number; error?: string } | null>(null);
  const [postingNow, setPostingNow] = useState(false);
  const [postNowResult, setPostNowResult] = useState<{ ok: boolean; url?: string; error?: string } | null>(null);
  const [transcriptOpen, setTranscriptOpen] = useState(false);

  const tweetText = caption;

  async function doPublish() {
    if (!clip.clip_path || !tweetText.trim()) return;
    setPublishing(true);
    setResult(null);
    try {
      const r = await api.streamersApprove(
        clip.clip_path, tweetText, clip.clip_id, clip.title,
        clip.source, clip.streamer, clip.url, clip.thumbnail_url, clip.x_handle,
        clip.view_count, clip.duration, clip.created_at,
      );
      setResult({ ok: true, position: r.position });
      setTimeout(() => onPublished(clip.clip_id ?? ""), 1200);
    } catch (e) {
      setResult({ ok: false, error: String(e) });
    } finally {
      setPublishing(false);
    }
  }

  async function doPostNow() {
    if (!clip.clip_path || !tweetText.trim()) return;
    setPostingNow(true);
    setPostNowResult(null);
    try {
      const r = await api.streamersPublish(
        clip.clip_path, tweetText, clip.clip_id, clip.title,
        clip.source, clip.streamer, clip.url, clip.thumbnail_url, clip.x_handle,
      );
      setPostNowResult({ ok: true, url: r.url });
      setTimeout(() => onPostNow(clip.clip_id ?? ""), 6000);
    } catch (e) {
      setPostNowResult({ ok: false, error: String(e) });
    } finally {
      setPostingNow(false);
    }
  }

  async function doSkip() {
    if (clip.clip_id) {
      try { await api.streamersSkip(clip.clip_id); } catch {}
    }
    onSkip(clip.clip_id ?? "");
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
            {clip.view_count != null && (
              <span>· {clip.view_count.toLocaleString()} views</span>
            )}
            {clip.created_at && (
              <span>· {new Date(clip.created_at).toLocaleDateString()}</span>
            )}
          </div>
          {/* Posting paths (#280): what Approve will queue for this streamer */}
          {clip.paths && (
            <div className="flex items-center gap-1 flex-wrap text-[10px]">
              <PathPill on={clip.paths.clip} label="Clip" title="clip_enabled — Approve queues the MP4" />
              <PathPill on={clip.paths.gif} label="GIF" title="gif_enabled — a reaction GIF is cut" />
              <PathPill on={!!clip.paths.gif_post} label="GIF→X" title="gif_post_enabled — Approve also queues the GIF to X" />
              <span className="text-muted ml-1">Approve → {approveLabel(clip.paths)}</span>
            </div>
          )}
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

      {/* Caption (editable) — left; the brain's raw door answer — right */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
        <div className="space-y-1">
          <p className="text-[10px] uppercase tracking-wide text-muted">
            caption ({clip.caption_mode === "brain" ? "brain, Spark" : clip.caption_mode || "3B"}) — what gets posted
          </p>
          <textarea
            rows={4}
            value={caption}
            onChange={(e) => setCaption(e.target.value)}
            className="w-full bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text resize-y"
          />
        </div>
        <div className="space-y-1">
          <p className="text-[10px] uppercase tracking-wide text-muted">
            brain (Spark) — {clip.caption_mode === "brain" ? "raw answer, promoted into the caption" : "not promoted for this clip"}
          </p>
          <p className="text-xs font-mono border border-border rounded p-2 bg-panel min-h-[5.5rem] whitespace-pre-wrap">
            {clip.brain_caption?.trim() ? (
              <span className="text-text">{clip.brain_caption}</span>
            ) : (
              <span className="text-muted">
                {typeof clip.brain?.error === "string"
                  ? `no brain caption — ${clip.brain.error}`
                  : "no brain caption"}
              </span>
            )}
          </p>
        </div>
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
          disabled={publishing || postingNow || !tweetText.trim() || !clip.clip_path}
        >
          {publishing ? "Queuing…" : clip.paths ? `Approve (${approveLabel(clip.paths)})` : "Approve"}
        </Button>
        <Button
          onClick={doPostNow}
          disabled={publishing || postingNow || !tweetText.trim() || !clip.clip_path}
          className="opacity-90"
          title="Posts the MP4 to X right now, bypassing the queue"
        >
          {postingNow ? "Posting…" : "Post Clip"}
        </Button>
        <Button
          onClick={doSkip}
          disabled={publishing || postingNow}
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
        {postNowResult && (
          <span className={postNowResult.ok ? "text-accent text-xs" : "text-bad text-xs"}>
            {postNowResult.ok ? (
              <>
                Posted ✓{" "}
                <a href={postNowResult.url} target="_blank" rel="noopener noreferrer" className="underline">
                  {postNowResult.url}
                </a>
              </>
            ) : (
              postNowResult.error
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
  onPostedNow,
}: {
  pending: PendingClip[];
  loading: boolean;
  onCancel: (clip_id: string) => void;
  onPostedNow: (clip_id: string) => void;
}) {
  const [cancelingId, setCancelingId] = useState<string | null>(null);
  const [postingId, setPostingId] = useState<string | null>(null);
  const [postResult, setPostResult] = useState<Record<string, { ok: boolean; url?: string; error?: string }>>({});

  async function doCancel(clip_id: string) {
    setCancelingId(clip_id);
    try {
      await api.streamersCancelPending(clip_id);
      onCancel(clip_id);
    } finally {
      setCancelingId(null);
    }
  }

  async function doPostNow(clip_id: string) {
    setPostingId(clip_id);
    try {
      const r = await api.streamersPendingPublishNow(clip_id);
      if (r.published === false) {
        setPostResult((prev) => ({ ...prev, [clip_id]: { ok: false, error: r.reason || "not in queue" } }));
      } else {
        setPostResult((prev) => ({ ...prev, [clip_id]: { ok: true, url: r.url } }));
        setTimeout(() => onPostedNow(clip_id), 6000);
      }
    } catch (e) {
      setPostResult((prev) => ({ ...prev, [clip_id]: { ok: false, error: String(e) } }));
    } finally {
      setPostingId(null);
    }
  }

  if (loading) return <p className="text-muted text-sm">Loading pending publish queue…</p>;
  if (pending.length === 0) return <p className="text-muted text-sm">Queue empty — nothing waiting to post.</p>;

  return (
    <div className="space-y-2">
      {pending.map((p, i) => {
        const result = postResult[p.clip_id];
        // A gif-path entry (#280): approve_clip queues it as "{clip_id}-gif" → .gif,
        // so show the GIF itself, not the clip's video thumbnail.
        const isGif = p.clip_id.endsWith("-gif") || (p.clip_path ?? "").endsWith(".gif");
        const baseId = isGif && p.clip_id.endsWith("-gif") ? p.clip_id.slice(0, -4) : p.clip_id;
        return (
          <div
            key={p.clip_id || i}
            className="flex items-start justify-between gap-3 border border-border rounded p-3 bg-bg"
          >
            {isGif ? (
              <img
                src={`/api/streamers/gif/${encodeURIComponent(baseId)}`}
                alt="gif"
                loading="lazy"
                className="w-20 h-12 object-cover rounded border border-border shrink-0"
              />
            ) : p.thumbnail_url && (
              <img
                src={p.thumbnail_url}
                alt="thumbnail"
                loading="lazy"
                className="w-20 h-12 object-cover rounded border border-border shrink-0"
              />
            )}
            <div className="min-w-0 flex-1 space-y-1">
              <div className="flex items-center gap-2 text-xs text-muted flex-wrap">
                <span className="font-semibold text-text">#{i + 1}</span>
                {isGif ? <Badge tone="warn">GIF</Badge> : <Badge tone="neutral">CLIP</Badge>}
                {p.source && <PlatformBadge platform={p.source as "twitch" | "kick"} />}
                {p.streamer && (
                  <a
                    href={p.source === "kick" ? `https://kick.com/${p.streamer}` : `https://www.twitch.tv/${p.streamer}`}
                    target="_blank" rel="noopener noreferrer"
                    className="text-text hover:text-accent font-mono"
                  >
                    {p.streamer}
                  </a>
                )}
                {p.x_handle && (
                  <a href={`https://x.com/${p.x_handle}`} target="_blank" rel="noopener noreferrer"
                     className="text-accent hover:underline">
                    @{p.x_handle}
                  </a>
                )}
                {p.duration != null && <span>· {Math.round(p.duration)}s</span>}
                {p.view_count != null && (
                  <span>· {p.view_count.toLocaleString()} views</span>
                )}
                {p.created_at && (
                  <span>· {new Date(p.created_at).toLocaleDateString()}</span>
                )}
              </div>
              {p.url ? (
                <a href={p.url} target="_blank" rel="noopener noreferrer"
                   className="text-xs font-semibold text-text hover:text-accent block truncate">
                  {p.title || p.clip_id || "Untitled Clip"}
                </a>
              ) : (
                <p className="text-xs font-semibold text-text truncate">{p.title || p.clip_id || "unknown clip"}</p>
              )}
              <p className="text-xs text-text whitespace-pre-wrap">{p.tweet_text}</p>
              {result && (
                <span className={result.ok ? "text-accent text-xs" : "text-bad text-xs"}>
                  {result.ok ? (
                    <>
                      Posted ✓{" "}
                      <a href={result.url} target="_blank" rel="noopener noreferrer" className="underline">
                        {result.url}
                      </a>
                    </>
                  ) : (
                    result.error
                  )}
                </span>
              )}
            </div>
            <div className="flex flex-col gap-1 shrink-0">
              <Button
                onClick={() => doPostNow(p.clip_id)}
                disabled={postingId === p.clip_id || cancelingId === p.clip_id}
                className="text-xs opacity-90"
              >
                {postingId === p.clip_id ? "Posting…" : isGif ? "Post GIF" : "Post Clip"}
              </Button>
              <Button
                onClick={() => doCancel(p.clip_id)}
                disabled={cancelingId === p.clip_id || postingId === p.clip_id}
                className="text-xs opacity-60"
              >
                {cancelingId === p.clip_id ? "Canceling…" : "Cancel"}
              </Button>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── PostedClipsPanel ───────────────────────────────────────────────────────

function PostedClipsPanel({ posted, loading }: { posted: PostedClip[]; loading: boolean }) {
  if (loading) return <p className="text-muted text-sm">Loading posted clips…</p>;
  if (posted.length === 0) return <p className="text-muted text-sm">Nothing posted yet.</p>;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
      {posted.map((p, i) => (
        <div key={p.clip_id || i} className="border border-border rounded overflow-hidden bg-bg flex flex-col">
          {p.thumbnail_url ? (
            <img src={p.thumbnail_url} alt="thumbnail" loading="lazy" className="w-full aspect-video object-cover" />
          ) : (
            <div className="w-full aspect-video bg-panel" />
          )}
          <div className="p-2 space-y-1 min-w-0">
            <div className="flex items-center gap-1.5 text-[11px] text-muted flex-wrap">
              {p.source && <PlatformBadge platform={p.source as "twitch" | "kick"} />}
              {p.streamer && <span className="font-mono truncate">{p.streamer}</span>}
            </div>
            <p className="text-xs font-semibold text-text line-clamp-2">{p.title || p.clip_id || "Untitled Clip"}</p>
            {p.published_at && (
              <p className="text-[10px] text-muted">{new Date(p.published_at).toLocaleString()}</p>
            )}
            {p.tweet_url && (
              <a href={p.tweet_url} target="_blank" rel="noopener noreferrer"
                 className="text-[11px] text-accent hover:underline block truncate">
                View post →
              </a>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── GifsPanel ──────────────────────────────────────────────────────────────

function GifsPanel({
  items,
  loading,
  onReviewed,
  onPosted,
}: {
  items: StreamerGif[];
  loading: boolean;
  onReviewed: (clip_id: string, verdict: "good" | "hidden") => void;
  onPosted: (clip_id: string) => void;
}) {
  const [reviewingId, setReviewingId] = useState<string | null>(null);
  const [postingId, setPostingId] = useState<string | null>(null);
  const [postResult, setPostResult] = useState<Record<string, { ok: boolean; url?: string; error?: string }>>({});

  async function doReview(clip_id: string, verdict: "good" | "hidden") {
    setReviewingId(clip_id);
    try {
      await api.streamersGifReview(clip_id, verdict);
      onReviewed(clip_id, verdict);
    } catch {} finally {
      setReviewingId(null);
    }
  }

  async function doPostNow(clip_id: string) {
    setPostingId(clip_id);
    try {
      const r = await api.streamersGifPostNow(clip_id);
      if (r.published === false) {
        setPostResult((prev) => ({ ...prev, [clip_id]: { ok: false, error: r.reason || "not published" } }));
      } else {
        setPostResult((prev) => ({ ...prev, [clip_id]: { ok: true, url: r.url } }));
        // Deliberately does NOT drop the card: the library is the archive of
        // what we've made, so a posted gif stays on the shelf. Only ❌ hides.
        onPosted(clip_id);
      }
    } catch (e) {
      setPostResult((prev) => ({ ...prev, [clip_id]: { ok: false, error: String(e) } }));
    } finally {
      setPostingId(null);
    }
  }

  if (loading) return <p className="text-muted text-sm">Loading GIFs…</p>;
  if (items.length === 0) return <p className="text-muted text-sm">No GIFs cut yet.</p>;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
      {items.map((g, i) => {
        const result = postResult[g.clip_id];
        const busy = reviewingId === g.clip_id || postingId === g.clip_id;
        const hidden = g.verdict === "hidden";
        return (
          <div
            key={g.clip_id || i}
            className={`border rounded overflow-hidden bg-bg flex flex-col ${
              hidden ? "border-border opacity-40 grayscale" : "border-border"
            }`}
          >
            <img
              /* indexed_at changes on every re-cut, so a recut gif gets a new
                 URL instead of hiding behind a cached one. */
              src={`/api/streamers/gif/${encodeURIComponent(g.clip_id)}?v=${encodeURIComponent(g.indexed_at || "")}`}
              alt={g.title || g.clip_id}
              loading="lazy"
              /* No forced aspect: the tile takes the gif's own shape, so what
                 you review is exactly what gets posted. aspect-video +
                 object-cover was 16:9-cropping the square cuts. */
              className="w-full h-auto block bg-panel"
            />
            <div className="p-2 space-y-1 min-w-0 flex-1 flex flex-col">
              <div className="flex items-center gap-1.5 text-[11px] text-muted flex-wrap">
                {g.source && <PlatformBadge platform={g.source as "twitch" | "kick"} />}
                {g.streamer && <span className="font-mono truncate text-text">{g.streamer}</span>}
                {g.view_count != null && <span>· {g.view_count.toLocaleString()} views</span>}
              </div>
              {g.url ? (
                <a
                  href={g.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs font-semibold text-text hover:text-accent line-clamp-2"
                >
                  {g.title || g.clip_id || "Untitled Clip"}
                </a>
              ) : (
                <p className="text-xs font-semibold text-text line-clamp-2">
                  {g.title || g.clip_id || "Untitled Clip"}
                </p>
              )}
              <div className="flex items-center gap-1.5 text-[10px] text-muted flex-wrap">
                <span>{(g.gif_bytes / 1048576).toFixed(1)} MB</span>
                {g.verdict === "good" && <Badge tone="ok">good</Badge>}
                {hidden && <Badge tone="neutral">hidden</Badge>}
                {g.tweet_url && (
                  <a href={g.tweet_url} target="_blank" rel="noopener noreferrer" title="View the post on X">
                    <Badge tone="ok">posted</Badge>
                  </a>
                )}
              </div>
              {g.crop_why && <p className="text-[10px] text-muted line-clamp-2">{g.crop_why}</p>}
              {g.gif_error && <p className="text-[10px] text-bad line-clamp-2">{g.gif_error}</p>}
              <div className="flex items-center gap-1 flex-wrap pt-1 mt-auto">
                <Button
                  onClick={() => doReview(g.clip_id, "good")}
                  disabled={busy}
                  variant="ghost"
                  className="text-[11px] px-2 py-1"
                >
                  ✅ good
                </Button>
                <Button
                  onClick={() => doReview(g.clip_id, "hidden")}
                  disabled={busy}
                  variant="ghost"
                  className="text-[11px] px-2 py-1 opacity-70"
                >
                  ❌ hide
                </Button>
                <Button
                  onClick={() => doPostNow(g.clip_id)}
                  disabled={busy}
                  className="text-[11px] px-2 py-1"
                >
                  {postingId === g.clip_id ? "Posting…" : g.tweet_url ? "Post GIF again" : "Post GIF"}
                </Button>
              </div>
              {result && (
                <span className={result.ok ? "text-accent text-[11px]" : "text-bad text-[11px]"}>
                  {result.ok ? (
                    <>
                      Posted ✓{" "}
                      <a href={result.url} target="_blank" rel="noopener noreferrer" className="underline break-all">
                        {result.url}
                      </a>
                    </>
                  ) : (
                    result.error
                  )}
                </span>
              )}
            </div>
          </div>
        );
      })}
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

// ── Roster grid — the "Watchlist" sub-tab (#279) ───────────────────────────
// The whole Postgres `streamer` table (roster_store), one row per streamer,
// inline edit / soft-delete / hard-delete / add / pin-to-feed. Not to be
// confused with `WatchList` below, the 4-entry FetchClips pin list.

type RosterDraft = {
  x_handle: string;
  display_name: string;
  aliases: string; // comma-separated while editing
  pronouns: string;
  notes: string;
  clip_enabled: boolean;
  gif_enabled: boolean;
  gif_post_enabled: boolean;
};

function draftFrom(r: RosterRow): RosterDraft {
  return {
    x_handle: r.x_handle, display_name: r.display_name, aliases: r.aliases.join(", "),
    pronouns: r.pronouns, notes: r.notes, clip_enabled: r.clip_enabled,
    gif_enabled: r.gif_enabled, gif_post_enabled: r.gif_post_enabled,
  };
}

function fmtTs(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function FlagPill({ on, label, editing, onToggle }: { on: boolean; label: string; editing: boolean; onToggle: () => void }) {
  const cls = on ? "bg-accent/20 text-accent border-accent/40" : "bg-bg text-muted border-border";
  return (
    <button
      type="button"
      disabled={!editing}
      onClick={onToggle}
      title={label}
      className={`text-[10px] px-1.5 py-0.5 rounded border ${cls} ${editing ? "cursor-pointer hover:text-text" : "cursor-default"}`}
    >
      {on ? "on" : "off"}
    </button>
  );
}

function RosterGrid() {
  const [rows, setRows] = useState<RosterRow[]>([]);
  const [watchlist, setWatchlist] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [unavailable, setUnavailable] = useState(false);
  const [showInactive, setShowInactive] = useState(false);
  const [editing, setEditing] = useState<string | null>(null); // `${platform}/${login}`
  const [draft, setDraft] = useState<RosterDraft | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [addLogin, setAddLogin] = useState("");
  const [addPlatform, setAddPlatform] = useState<"twitch" | "kick">("twitch");

  const refresh = async () => {
    try {
      const r = await api.streamersRosterRows();
      setRows(r.rows);
      setWatchlist(r.watchlist);
      setUnavailable(!r.ok);
    } catch (e) {
      setMsg(`load failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { refresh(); }, []);

  const key = (r: RosterRow) => `${r.platform}/${r.login}`;

  async function run(label: string, k: string, fn: () => Promise<{ ok: boolean; message?: string; reason?: string }>) {
    setBusy(k);
    setMsg(null);
    try {
      const res = await fn();
      setMsg(res.message ?? (res.ok ? `${label}: ok` : `${label}: ${res.reason ?? "failed"}`));
      await refresh();
    } catch (e) {
      setMsg(`${label} failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(null);
    }
  }

  function startEdit(r: RosterRow) {
    setEditing(key(r));
    setDraft(draftFrom(r));
  }

  async function saveEdit(r: RosterRow) {
    if (!draft) return;
    const patch: RosterPatch = {
      display_name: draft.display_name.trim(),
      aliases: draft.aliases.split(",").map((a) => a.trim()).filter(Boolean),
      notes: draft.notes.trim(),
      clip_enabled: draft.clip_enabled,
      gif_enabled: draft.gif_enabled,
      gif_post_enabled: draft.gif_post_enabled,
    };
    const newHandle = draft.x_handle.trim().replace(/^@/, "");
    if (newHandle !== r.x_handle) {
      // A human typed a handle — that is the confirmation the chat path couldn't get.
      patch.x_handle = newHandle;
      patch.x_handle_status = newHandle ? "confirmed" : "";
    }
    const newPronouns = draft.pronouns.trim();
    if (newPronouns !== r.pronouns) {
      // Typing pronouns stores them as needs_review; the explicit Confirm button
      // flips them to confirmed — only confirmed values reach the Spark brain.
      patch.pronouns = newPronouns;
      patch.pronouns_status = newPronouns ? "needs_review" : "";
    }
    await run("save", key(r), () => api.streamersRosterUpdate(r.platform, r.login, patch));
    setEditing(null);
    setDraft(null);
  }

  async function add() {
    const login = addLogin.trim().replace(/^@/, "").toLowerCase();
    if (!login) return;
    await run("add", `add/${login}`, () => api.streamersRosterAdd(login, addPlatform));
    setAddLogin("");
  }

  const visible = rows.filter((r) => showInactive || r.active);
  const needsReview = rows.filter((r) => r.active && r.x_handle_status === "needs_review").length;
  const cell = "px-2 py-1 align-top whitespace-nowrap";
  const input = "bg-bg border border-border rounded px-1 py-0.5 text-xs font-mono text-text w-full min-w-[6rem]";

  return (
    <Card>
      <div className="flex items-center justify-between flex-wrap gap-2 mb-2">
        <CardTitle>Watchlist — roster ({rows.filter((r) => r.active).length} active / {rows.length} rows)</CardTitle>
        <div className="flex items-center gap-2 flex-wrap">
          {needsReview > 0 && <Badge tone="warn">{needsReview} needs review</Badge>}
          <label className="text-xs text-muted flex items-center gap-1">
            <input type="checkbox" checked={showInactive} onChange={(e) => setShowInactive(e.target.checked)} />
            show inactive
          </label>
          <Button variant="ghost" onClick={refresh} disabled={loading}>Refresh</Button>
        </div>
      </div>

      <p className="text-xs text-muted mb-2">
        Source of truth is the Postgres <code>streamer</code> table (#275). Edit → fields become inputs; Save writes
        the row and reloads the app's cache. A typed X handle counts as confirmed. Pronouns are typed as{" "}
        <em>needs review</em> and only reach the Spark brain once you press Confirm — nothing is ever inferred.
        Deactivate = the chat ➖ soft-delete; Delete really drops the row (test rows only). Pin/Unpin is the
        FetchClips feed list (the Overview watch list).
      </p>

      {/* Add row — the chat ➕ path's guards apply (channel must exist; handle confirmed-or-needs_review) */}
      <div className="flex items-center gap-2 flex-wrap mb-3">
        <input
          value={addLogin}
          onChange={(e) => setAddLogin(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") add(); }}
          placeholder="login"
          className="bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text w-48"
        />
        <PlatformToggle platform={addPlatform} onChange={setAddPlatform} />
        <Button onClick={add} disabled={busy !== null || !addLogin.trim() || unavailable}>Add</Button>
        {msg && <span className="text-xs text-muted">{msg}</span>}
      </div>

      {unavailable && (
        <p className="text-xs text-bad mb-2">roster store unreachable — showing nothing; the app is on its hardcoded fallback roster.</p>
      )}
      {loading ? (
        <p className="text-xs text-muted">Loading…</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="text-xs w-full border-collapse">
            <thead>
              <tr className="text-muted text-left border-b border-border">
                {["platform", "login", "display name", "aliases", "x handle", "pronouns", "notes", "clip", "gif", "gif post", "active", "feed", "added by", "source", "added", "updated", ""].map((h) => (
                  <th key={h} className={`${cell} font-semibold`}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visible.map((r) => {
                const k = key(r);
                const isEditing = editing === k && draft !== null;
                const pinned = watchlist.includes(r.entry);
                const review = r.x_handle_status === "needs_review";
                const rowCls = [
                  "border-b border-border/60",
                  review ? "border-warn/40 bg-warn/5" : "",
                  r.active ? "" : "opacity-50",
                ].join(" ");
                return (
                  <tr key={k} className={rowCls}>
                    <td className={cell}><PlatformBadge platform={r.platform} /></td>
                    <td className={`${cell} font-mono text-text`}>{r.login}</td>
                    <td className={cell}>
                      {isEditing ? <input className={input} value={draft.display_name} onChange={(e) => setDraft({ ...draft, display_name: e.target.value })} /> : (r.display_name || <span className="text-muted">—</span>)}
                    </td>
                    <td className={cell}>
                      {isEditing ? <input className={input} value={draft.aliases} placeholder="a, b" onChange={(e) => setDraft({ ...draft, aliases: e.target.value })} /> : (r.aliases.length ? r.aliases.join(", ") : <span className="text-muted">—</span>)}
                    </td>
                    <td className={cell}>
                      {isEditing ? (
                        <input className={input} value={draft.x_handle} onChange={(e) => setDraft({ ...draft, x_handle: e.target.value })} />
                      ) : (
                        <span className="flex items-center gap-1">
                          <span className="font-mono">{r.x_handle ? `@${r.x_handle}` : "—"}</span>
                          {review && <Badge tone="warn">needs review</Badge>}
                          {review && r.x_handle && (
                            <Button variant="ghost" disabled={busy !== null} onClick={() => run("confirm handle", k, () => api.streamersRosterUpdate(r.platform, r.login, { x_handle_status: "confirmed" }))}>Confirm</Button>
                          )}
                        </span>
                      )}
                    </td>
                    <td className={cell}>
                      {isEditing ? (
                        <input className={input} value={draft.pronouns} placeholder="she/her" onChange={(e) => setDraft({ ...draft, pronouns: e.target.value })} />
                      ) : (
                        <span className="flex items-center gap-1">
                          <span className="font-mono">{r.pronouns || "—"}</span>
                          {r.pronouns && r.pronouns_status === "confirmed" && <Badge tone="ok">confirmed</Badge>}
                          {r.pronouns && r.pronouns_status !== "confirmed" && (
                            <>
                              <Badge tone="warn">needs review</Badge>
                              <Button variant="ghost" disabled={busy !== null} onClick={() => run("confirm pronouns", k, () => api.streamersRosterUpdate(r.platform, r.login, { pronouns_status: "confirmed" }))}>Confirm</Button>
                            </>
                          )}
                        </span>
                      )}
                    </td>
                    <td className={`${cell} max-w-[16rem] whitespace-normal`}>
                      {isEditing ? <input className={input} value={draft.notes} onChange={(e) => setDraft({ ...draft, notes: e.target.value })} /> : (r.notes || <span className="text-muted">—</span>)}
                    </td>
                    <td className={cell}><FlagPill label="clip_enabled" on={isEditing ? draft.clip_enabled : r.clip_enabled} editing={isEditing} onToggle={() => draft && setDraft({ ...draft, clip_enabled: !draft.clip_enabled })} /></td>
                    <td className={cell}><FlagPill label="gif_enabled" on={isEditing ? draft.gif_enabled : r.gif_enabled} editing={isEditing} onToggle={() => draft && setDraft({ ...draft, gif_enabled: !draft.gif_enabled })} /></td>
                    <td className={cell}><FlagPill label="gif_post_enabled" on={isEditing ? draft.gif_post_enabled : r.gif_post_enabled} editing={isEditing} onToggle={() => draft && setDraft({ ...draft, gif_post_enabled: !draft.gif_post_enabled })} /></td>
                    <td className={cell}>{r.active ? <Badge tone="ok">active</Badge> : <Badge tone="neutral">inactive</Badge>}</td>
                    <td className={cell}>
                      {pinned ? <Badge tone="ok">pinned</Badge> : <span className="text-muted">—</span>}
                    </td>
                    <td className={`${cell} text-muted`}>{r.added_by || "—"}</td>
                    <td className={`${cell} text-muted`}>{r.source}</td>
                    <td className={`${cell} text-muted`}>{fmtTs(r.added_at)}</td>
                    <td className={`${cell} text-muted`}>{fmtTs(r.updated_at)}</td>
                    <td className={cell}>
                      <div className="flex items-center gap-1 flex-wrap">
                        {isEditing ? (
                          <>
                            <Button disabled={busy !== null} onClick={() => saveEdit(r)}>Save</Button>
                            <Button variant="ghost" disabled={busy !== null} onClick={() => { setEditing(null); setDraft(null); }}>Cancel</Button>
                          </>
                        ) : (
                          <>
                            <Button variant="ghost" disabled={busy !== null || !r.active} onClick={() => startEdit(r)}>Edit</Button>
                            {pinned ? (
                              <Button variant="ghost" disabled={busy !== null} onClick={() => run("unpin", k, async () => { await api.streamersWatchlistRemove(r.login, r.platform); return { ok: true, message: `${r.entry} unpinned from the feed list` }; })}>Unpin</Button>
                            ) : (
                              <Button variant="ghost" disabled={busy !== null || !r.active} onClick={() => run("pin", k, async () => { await api.streamersWatchlistAdd(r.login, r.platform); return { ok: true, message: `${r.entry} pinned to the feed list` }; })}>Pin</Button>
                            )}
                            {r.active ? (
                              <Button variant="ghost" disabled={busy !== null} onClick={() => run("deactivate", k, () => api.streamersRosterDelete(r.platform, r.login, false).then((x) => ({ ok: x.ok, message: x.removed ? `${r.entry} deactivated (soft-delete)` : `${r.entry} was not active` })))}>Deactivate</Button>
                            ) : (
                              <Button variant="ghost" disabled={busy !== null} onClick={() => run("reactivate", k, () => api.streamersRosterUpdate(r.platform, r.login, { active: true }))}>Reactivate</Button>
                            )}
                            <Button
                              variant="danger"
                              disabled={busy !== null}
                              onClick={() => {
                                if (window.confirm(`Really DELETE the row for ${r.entry}? This is the hard delete — history is gone. Use Deactivate for a normal remove.`)) {
                                  run("delete", k, () => api.streamersRosterDelete(r.platform, r.login, true).then((x) => ({ ok: x.ok, message: x.removed ? `${r.entry} deleted` : `${r.entry} not found` })));
                                }
                              }}
                            >
                              Delete
                            </Button>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
              {visible.length === 0 && (
                <tr><td className={`${cell} text-muted`} colSpan={17}>No rows{showInactive ? "" : " (inactive hidden)"}.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function WatchList() {
  const [logins, setLogins] = useState<string[]>([]);
  const [input, setInput] = useState("");
  const [platform, setPlatform] = useState<"twitch" | "kick">("twitch");
  const [saving, setSaving] = useState(false);
  const [rotating, setRotating] = useState(false);
  const [fetchMode, setFetchMode] = useState<{ mode: string; period: string }>({ mode: "recent", period: "month" });
  const [liveStatus, setLiveStatus] = useState<Record<string, boolean>>({});
  const [liveLoading, setLiveLoading] = useState(false);
  const [triggeringAlert, setTriggeringAlert] = useState(false);
  const [triggerAlertResult, setTriggerAlertResult] = useState<string | null>(null);

  const refreshLiveStatus = async (forLogins: string[]) => {
    if (forLogins.length === 0) { setLiveStatus({}); return; }
    setLiveLoading(true);
    try {
      const r = await api.streamersLiveBulk(forLogins);
      setLiveStatus(r.statuses);
    } catch {} finally {
      setLiveLoading(false);
    }
  };

  useEffect(() => {
    api.streamersWatchlist().then((r) => { setLogins(r.logins); refreshLiveStatus(r.logins); }).catch(() => {});
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
      refreshLiveStatus(r.logins);
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
      refreshLiveStatus(r.logins);
    } finally {
      setSaving(false);
    }
  }

  async function rotate() {
    setRotating(true);
    try {
      const r = await api.streamersRotateWatchlist();
      setLogins(r.logins);
      refreshLiveStatus(r.logins);
    } finally {
      setRotating(false);
    }
  }

  async function triggerLiveStreamerAlert() {
    setTriggeringAlert(true);
    setTriggerAlertResult(null);
    try {
      await api.streamersTrigger("LiveStreamerAlert");
      setTriggerAlertResult("Triggered LiveStreamerAlert.");
    } catch (e) {
      setTriggerAlertResult(`Error: ${String(e)}`);
    } finally {
      setTriggeringAlert(false);
    }
  }

  return (
    <Card>
      <div className="flex items-center justify-between mb-3">
        <CardTitle className="mb-0">Watch List</CardTitle>
        <div className="flex items-center gap-2">
          <Button className="text-xs" onClick={() => refreshLiveStatus(logins)} disabled={liveLoading}>
            {liveLoading ? "Checking…" : "Refresh Status"}
          </Button>
          <Button onClick={rotate} disabled={rotating}>
            {rotating ? "Rotating…" : "Rotate"}
          </Button>
        </div>
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
                  {login in liveStatus && (
                    <Badge tone={liveStatus[login] ? "ok" : "neutral"}>
                      {liveStatus[login] ? "LIVE" : "offline"}
                    </Badge>
                  )}
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
        <div className="pt-2 border-t border-border flex items-center gap-3">
          <Button className="text-xs" onClick={triggerLiveStreamerAlert} disabled={triggeringAlert}>
            {triggeringAlert ? "Triggering…" : "Trigger LiveStreamerAlert"}
          </Button>
          {triggerAlertResult && (
            <p className={`text-xs ${triggerAlertResult.startsWith("Error") ? "text-bad" : "text-accent"}`}>{triggerAlertResult}</p>
          )}
        </div>
      </div>
    </Card>
  );
}

// ── Inspector ──────────────────────────────────────────────────────────────

function ChatterRow({ c, rank }: { c: InspectorChatter; rank?: number }) {
  const noBadges = !c.is_bot && c.badges.length === 0;
  return (
    <div className="flex items-start justify-between gap-2 border-b border-border/60 py-1.5 last:border-0">
      <div className="min-w-0">
        <div className="flex items-center gap-1.5 flex-wrap">
          {rank !== undefined && <span className="text-[10px] text-muted font-mono w-5 shrink-0">#{rank}</span>}
          <span className="font-mono text-sm text-text">{c.username}</span>
          {c.badges.map((b, i) => (
            <span key={i} className="text-[10px] px-1 py-0.5 rounded border border-border text-muted">
              {b}
            </span>
          ))}
          {noBadges && (
            <span className="text-[10px] px-1 py-0.5 rounded border border-warn/40 text-warn" title="No badges seen — newer or low-investment account, not necessarily fake">
              no badges
            </span>
          )}
          {!!c.cross_channel_count && c.cross_channel_count > 0 && (
            <span
              className="text-[10px] px-1 py-0.5 rounded border border-bad/40 text-bad"
              title="Active chatter in multiple other watchlisted channels recently — a real bot-farm signal, but also just an active community member who follows several of these streamers"
            >
              seen in {c.cross_channel_count} other channel{c.cross_channel_count === 1 ? "" : "s"}
            </span>
          )}
        </div>
        {c.samples.length > 0 && (
          <p className="text-xs text-muted truncate mt-0.5">{c.samples[0]}</p>
        )}
      </div>
      <span className="text-xs text-muted shrink-0">{c.message_count} msg{c.message_count === 1 ? "" : "s"}</span>
    </div>
  );
}

function InspectorClipCard({
  clip,
  platform,
  streamer,
}: {
  clip: InspectorClip;
  platform?: "twitch" | "kick";
  streamer?: string;
}) {
  const [queueState, setQueueState] = useState<"idle" | "queuing" | "queued" | "error">("idle");
  const [queueError, setQueueError] = useState<string | null>(null);

  const doQueue = async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (!platform || !streamer || !clip.clip_id) return;
    setQueueState("queuing");
    setQueueError(null);
    try {
      const r = await api.streamersQueueClip(
        platform, streamer, clip.clip_id, clip.url ?? "", clip.thumbnail_url ?? "",
        clip.title ?? "", clip.view_count ?? 0, clip.created_at ?? "",
      );
      if (r.ok) {
        setQueueState("queued");
      } else {
        setQueueState("error");
        setQueueError(r.error ?? "unknown error");
      }
    } catch (err) {
      setQueueState("error");
      setQueueError(String(err));
    }
  };

  return (
    <a
      href={clip.url}
      target="_blank"
      rel="noreferrer"
      className="border border-border rounded overflow-hidden bg-bg hover:border-accent/60 transition-colors block"
    >
      {clip.thumbnail_url && (
        <img src={clip.thumbnail_url} alt={clip.title ?? ""} className="w-full aspect-video object-cover" />
      )}
      <div className="p-2">
        <p className="text-xs text-text truncate">{clip.title || "(untitled)"}</p>
        <div className="flex items-center justify-between text-[10px] text-muted mt-1">
          <span>{clip.duration ? `${Math.round(clip.duration)}s` : "?"}</span>
          <span>{clip.view_count ?? 0} views</span>
        </div>
        {platform && streamer && (
          <Button
            onClick={doQueue}
            disabled={queueState === "queuing" || queueState === "queued"}
            className="w-full mt-1.5 text-[10px] py-1"
          >
            {queueState === "idle" && "Queue this clip"}
            {queueState === "queuing" && "Processing…"}
            {queueState === "queued" && "Queued ✓"}
            {queueState === "error" && "Failed — retry"}
          </Button>
        )}
        {queueError && <p className="text-[10px] text-bad mt-1">{queueError}</p>}
      </div>
    </a>
  );
}

type UsersBotsTarget = { platform: "twitch" | "kick"; login: string; nonce: number };

function LiveNowPicker({ onPick }: { onPick: (platform: "twitch" | "kick", login: string) => void }) {
  const [liveNow, setLiveNow] = useState<string[]>([]);
  const [liveNowLoading, setLiveNowLoading] = useState(true);

  const refreshLiveNow = async () => {
    setLiveNowLoading(true);
    try {
      const r = await api.streamersLiveNow();
      setLiveNow(r.live);
    } catch {} finally {
      setLiveNowLoading(false);
    }
  };

  useEffect(() => {
    refreshLiveNow();
  }, []);

  return (
    <Card>
      <div className="flex items-center justify-between mb-3">
        <CardTitle className="mb-0">Live Now</CardTitle>
        <Button className="text-xs" onClick={refreshLiveNow} disabled={liveNowLoading}>
          {liveNowLoading ? "Checking…" : "Refresh"}
        </Button>
      </div>
      {liveNow.length === 0 ? (
        <p className="text-xs text-muted">{liveNowLoading ? "Checking roster…" : "Nobody in the roster is live right now."}</p>
      ) : (
        <div className="flex flex-wrap gap-2">
          {liveNow.map((entry) => {
            const isKick = entry.startsWith("kick:");
            const name = isKick ? entry.slice(5) : entry;
            return (
              <button
                key={entry}
                onClick={() => onPick(isKick ? "kick" : "twitch", name)}
                className="flex items-center gap-1.5 border border-accent/40 bg-accent/5 hover:bg-accent/15 rounded px-2 py-1 text-xs font-mono transition-colors"
              >
                <PlatformBadge platform={isKick ? "kick" : "twitch"} />
                <span className="text-text">{name}</span>
                <Badge tone="ok">LIVE</Badge>
              </button>
            );
          })}
        </div>
      )}
    </Card>
  );
}

function PlatformToggle({
  platform,
  onChange,
}: {
  platform: "twitch" | "kick";
  onChange: (p: "twitch" | "kick") => void;
}) {
  return (
    <div className="flex rounded border border-border overflow-hidden shrink-0 text-xs font-semibold">
      <button
        onClick={() => onChange("twitch")}
        className={`px-2 py-1 uppercase tracking-wide transition-colors ${
          platform === "twitch" ? "bg-purple-900 text-purple-200" : "bg-bg text-muted hover:text-text"
        }`}
      >
        Twitch
      </button>
      <button
        onClick={() => onChange("kick")}
        className={`px-2 py-1 uppercase tracking-wide transition-colors border-l border-border ${
          platform === "kick" ? "bg-green-800 text-green-200" : "bg-bg text-muted hover:text-text"
        }`}
      >
        Kick
      </button>
    </div>
  );
}

function LiveChannelEmbed({ platform, login }: { platform: "twitch" | "kick"; login: string }) {
  const src =
    platform === "twitch"
      ? `https://player.twitch.tv/?channel=${encodeURIComponent(login)}&parent=${window.location.hostname}&muted=true`
      : `https://player.kick.com/${encodeURIComponent(login)}`;
  const watchUrl = platform === "twitch" ? `https://www.twitch.tv/${login}` : `https://kick.com/${login}`;

  return (
    <div className="space-y-2">
      <div className="aspect-video w-full border border-border rounded overflow-hidden bg-black">
        <iframe
          src={src}
          allow="autoplay; fullscreen"
          className="w-full h-full"
          title={`${login} live`}
        />
      </div>
      <a href={watchUrl} target="_blank" rel="noreferrer" className="text-xs text-accent hover:underline">
        Watch on {platform === "twitch" ? "Twitch" : "Kick"} ↗
      </a>
    </div>
  );
}

function Inspector({ onOpenUsersBots }: { onOpenUsersBots: (platform: "twitch" | "kick", login: string) => void }) {
  const [login, setLogin] = useState("");
  const [platform, setPlatform] = useState<"twitch" | "kick">("twitch");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<InspectorResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const doInspect = async (overrideBare?: string, overridePlatform?: "twitch" | "kick") => {
    const plat = overridePlatform ?? platform;
    const bare = (overrideBare ?? login).trim().toLowerCase();
    if (!bare) return;
    setLogin(bare);
    setPlatform(plat);
    const target = plat === "kick" ? `kick:${bare}` : bare;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const r = await api.streamersInspect(target);
      setResult(r);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <LiveNowPicker onPick={(p, name) => doInspect(name, p)} />

      <Card>
        <CardTitle>Inspect a Streamer</CardTitle>
        <div className="flex flex-wrap items-center gap-2">
          <PlatformToggle platform={platform} onChange={setPlatform} />
          <input
            value={login}
            onChange={(e) => setLogin(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && doInspect()}
            placeholder={platform === "kick" ? "Kick slug (e.g. bbjess)" : "Twitch login (e.g. xqc)"}
            className="flex-1 min-w-[160px] bg-bg border border-border rounded px-2 py-1.5 text-sm text-text font-mono"
          />
          <Button onClick={() => doInspect()} disabled={loading || !login.trim()}>
            {loading ? "Inspecting…" : "Inspect"}
          </Button>
        </div>
        {error && <p className="text-xs text-bad mt-2">{error}</p>}
      </Card>

      {result && (
        <>
          <Card>
            <div className="flex items-center gap-2 flex-wrap">
              <PlatformBadge platform={result.platform} />
              <span className="font-mono text-sm text-text">{result.login}</span>
              <Badge tone={result.live ? "ok" : "neutral"}>{result.live ? "LIVE" : "OFFLINE"}</Badge>
              {result.live && (
                <Button className="text-xs ml-auto" onClick={() => onOpenUsersBots(result.platform, login)}>
                  Open Users/Bots for this channel →
                </Button>
              )}
            </div>
          </Card>

          {result.live && (
            <Card>
              <CardTitle>Live Channel View</CardTitle>
              <LiveChannelEmbed platform={result.platform} login={login} />
            </Card>
          )}

          <Card>
            <CardTitle>Recent Clips ({result.clips.length})</CardTitle>
            {result.clips.length === 0 ? (
              <p className="text-xs text-muted">No clips found.</p>
            ) : (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
                {result.clips.map((clip) => (
                  <InspectorClipCard
                    key={clip.clip_id}
                    clip={clip}
                    platform={result.platform}
                    streamer={result.login.startsWith("kick:") ? result.login.slice(5) : result.login}
                  />
                ))}
              </div>
            )}
          </Card>

          <Card>
            <CardTitle>What's Being Said</CardTitle>
            <p className="text-xs text-muted">
              Not built yet — cross-referencing X/web for what other people are saying about this streamer
              or their clips is scoped in <code>streamers-viral.md</code>, but needs real hands-on iteration
              (no usable free search API, public-web scraping is untested). Placeholder until that lands.
            </p>
          </Card>

          <Card>
            <CardTitle>
              Alt Platform Check — <span className="uppercase">{result.alt_platform.platform}</span>
            </CardTitle>
            {result.alt_platform.error ? (
              <p className="text-xs text-bad">Couldn't check: {result.alt_platform.error}</p>
            ) : result.alt_platform.exists === false ? (
              <p className="text-xs text-muted">
                No {result.alt_platform.platform} account found under "{login}".
              </p>
            ) : (
              <>
                <div className="flex items-center gap-2 mb-2">
                  <PlatformBadge platform={result.alt_platform.platform} />
                  <span className="font-mono text-sm text-text">{login}</span>
                  <Badge tone={result.alt_platform.live ? "ok" : "neutral"}>
                    {result.alt_platform.live ? "LIVE" : "OFFLINE"}
                  </Badge>
                  <span className="text-xs text-muted">also exists here</span>
                </div>
                {result.alt_platform.sample_clips && result.alt_platform.sample_clips.length > 0 && (
                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                    {result.alt_platform.sample_clips.map((clip) => (
                      <InspectorClipCard
                        key={clip.clip_id}
                        clip={clip}
                        platform={result.alt_platform.platform}
                        streamer={login}
                      />
                    ))}
                  </div>
                )}
              </>
            )}
          </Card>
        </>
      )}
    </div>
  );
}

// ── Users/Bots ───────────────────────────────────────────────────────────────

function UsersBots({ target }: { target: UsersBotsTarget | null }) {
  const [login, setLogin] = useState("");
  const [platform, setPlatform] = useState<"twitch" | "kick">("twitch");
  const [chatSeconds, setChatSeconds] = useState(25);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ChatInspectResult | ChatActivitySnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Whether `result` is coming from WatchlistChatSnapshotPoller's continuous
  // feed (Twitch-only, watchlisted channels) rather than a one-shot capture.
  const [live, setLive] = useState(false);
  const closeTail = useRef<(() => void) | null>(null);

  const stopTail = () => {
    closeTail.current?.();
    closeTail.current = null;
  };

  const doInspectChat = async (overrideBare?: string, overridePlatform?: "twitch" | "kick") => {
    const plat = overridePlatform ?? platform;
    const bare = (overrideBare ?? login).trim().toLowerCase();
    if (!bare) return;
    setLogin(bare);
    setPlatform(plat);
    stopTail();
    setLive(false);
    setLoading(true);
    setError(null);
    setResult(null);

    // Watchlisted Twitch channels get WatchlistChatSnapshotPoller's continuous
    // feed instead of a bounded one-shot capture. Kick isn't covered by that
    // poller yet (same Twitch-only boundary WatchlistChatJoiner has) — always
    // falls back to the one-shot path below.
    if (plat === "twitch") {
      try {
        const wl = await api.streamersWatchlist();
        if (wl.logins.includes(bare)) {
          setLive(true);
          try {
            setResult(await api.streamersChatActivity(bare));
          } catch {
            // No snapshot recorded yet (poller hasn't run a cycle for this
            // login) — still go live, the SSE tail below fills it in once
            // the next cycle publishes.
          }
          closeTail.current = openSSE(
            `/api/streamers/chat-activity/${encodeURIComponent(bare)}/tail`,
            (_evt, data) => {
              try {
                setResult(JSON.parse(data) as ChatActivitySnapshot);
              } catch {}
            },
          );
          setLoading(false);
          return;
        }
      } catch {
        // Watchlist lookup failed — fall through to the one-shot path.
      }
    }

    const t = plat === "kick" ? `kick:${bare}` : bare;
    try {
      const r = await api.streamersInspectChat(t, chatSeconds);
      setResult(r);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (target) doInspectChat(target.login, target.platform);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target?.nonce]);

  useEffect(() => stopTail, []);

  return (
    <div className="space-y-4">
      <LiveNowPicker onPick={(p, name) => doInspectChat(name, p)} />

      <Card>
        <CardTitle>Inspect a Channel</CardTitle>
        <div className="flex flex-wrap items-center gap-2">
          <PlatformToggle platform={platform} onChange={setPlatform} />
          <input
            value={login}
            onChange={(e) => setLogin(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && doInspectChat()}
            placeholder={platform === "kick" ? "Kick slug (e.g. bbjess)" : "Twitch login (e.g. xqc)"}
            className="flex-1 min-w-[160px] bg-bg border border-border rounded px-2 py-1.5 text-sm text-text font-mono"
          />
          <label className="text-xs text-muted flex items-center gap-1.5">
            Chat capture
            <input
              type="number"
              min={10}
              max={60}
              value={chatSeconds}
              onChange={(e) => setChatSeconds(Number(e.target.value))}
              className="w-16 bg-bg border border-border rounded px-1.5 py-1 text-sm text-text"
            />
            s
          </label>
          <Button onClick={() => doInspectChat()} disabled={loading || !login.trim()}>
            {loading ? (live ? "Loading…" : `Capturing… (~${chatSeconds}s)`) : "Inspect Channel"}
          </Button>
        </div>
        {error && <p className="text-xs text-bad mt-2">{error}</p>}
      </Card>

      {result && (
        <>
          <Card>
            <div className="flex items-center gap-2 flex-wrap mb-2">
              <PlatformBadge platform={result.platform} />
              <span className="font-mono text-sm text-text">{result.login}</span>
              <Badge tone={result.live ? "ok" : "neutral"}>{result.live ? "LIVE" : "OFFLINE"}</Badge>
              {live && (
                <span title="Continuously refreshed by WatchlistChatSnapshotPoller — not a one-shot capture">
                  <Badge tone="ok">AUTO-UPDATING</Badge>
                </span>
              )}
              {result.bot_flag_likely && (
                <span title="unique_chatters is under 10% of viewer_count at real scale — a cheap secondary signal, not a verdict on its own">
                  <Badge tone="bad">LOW ENGAGEMENT RATIO</Badge>
                </span>
              )}
            </div>
            {result.live && (
              <p className="text-xs text-muted">
                {result.viewer_count !== null ? (
                  <>
                    <span className="text-text font-semibold">{result.viewer_count.toLocaleString()}</span> viewers
                    reported by the platform, only <span className="text-text font-semibold">{result.unique_chatters}</span> unique
                    account{result.unique_chatters === 1 ? "" : "s"} actually spoke in a {result.duration_sec}s window
                    ({result.messages_seen} message{result.messages_seen === 1 ? "" : "s"} seen{result.message_cap_hit ? ", hit the capture cap" : ""}
                    {result.engagement_ratio !== null ? `, ${(result.engagement_ratio * 100).toFixed(1)}% engagement ratio` : ""}).
                    Most viewers lurking is completely normal — this ratio alone isn't evidence of anything fake.
                  </>
                ) : (
                  <>Viewer count unavailable — {result.messages_seen} message{result.messages_seen === 1 ? "" : "s"} seen from {result.unique_chatters} account{result.unique_chatters === 1 ? "" : "s"}.</>
                )}
              </p>
            )}
          </Card>

          {result.clusters.length > 0 && (
            <Card className="border-bad/40">
              <CardTitle>
                Possible Spam/Raid Clusters ({result.clusters.length})
              </CardTitle>
              <p className="text-xs text-muted mb-2">
                Same message text sent by several different accounts — a real signature of copypasta/raid
                spam or a bot farm, but a genuine hype wave from real fans can look identical. Treat as a
                lead to look closer at, not a verdict.
              </p>
              <div className="space-y-2">
                {result.clusters.map((cl, i) => (
                  <div key={i} className="border border-bad/30 bg-bad/5 rounded p-2">
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span className="text-text">"{cl.sample_text}"</span>
                      <span className="text-muted shrink-0 ml-2">{cl.distinct_senders} accounts, {cl.total_messages} msgs</span>
                    </div>
                    <p className="text-[10px] text-muted font-mono truncate">{cl.senders.join(", ")}</p>
                  </div>
                ))}
              </div>
            </Card>
          )}

          <Card>
            <CardTitle>
              Top Chatters — {result.unique_chatters} unique chatter{result.unique_chatters === 1 ? "" : "s"}
              {result.bots.length > 0 ? `, ${result.bots.length} bot${result.bots.length === 1 ? "" : "s"}` : ""}
            </CardTitle>
            {result.note && <p className="text-xs text-muted mb-2">{result.note}</p>}
            {result.error && <p className="text-xs text-bad mb-2">{result.error}</p>}
            {result.bots.length > 0 && (
              <div className="mb-3">
                <p className="text-xs uppercase tracking-wide text-muted mb-1">Bots</p>
                <div className="border border-warn/40 bg-warn/5 rounded p-2">
                  {result.bots.map((c) => (
                    <ChatterRow key={c.username} c={c} />
                  ))}
                </div>
              </div>
            )}
            {result.chatters.length > 0 && (
              <div>
                <p className="text-xs uppercase tracking-wide text-muted mb-1">
                  Ranked by message count (top {result.chatters.length})
                </p>
                <div className="max-h-96 overflow-y-auto border border-border rounded p-2">
                  {result.chatters.map((c, i) => (
                    <ChatterRow key={c.username} c={c} rank={i + 1} />
                  ))}
                </div>
              </div>
            )}
            {!result.note && !result.error && result.unique_chatters === 0 && (
              <p className="text-xs text-muted">Nobody chatted during the capture window.</p>
            )}
          </Card>
        </>
      )}
    </div>
  );
}

// ── StreamersPage ──────────────────────────────────────────────────────────

export function StreamersPage() {
  const [flows, setFlows] = useState<StreamerFlows>({});
  const [clips, setClips] = useState<StreamerClip[]>([]);
  const [clipsLoading, setClipsLoading] = useState(true);
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());
  const [topics, setTopics] = useState<StreamerTopics | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [peekOpen, setPeekOpen] = useState<Record<string, boolean>>({});
  const [resetting, setResetting] = useState(false);
  const [resetResult, setResetResult] = useState<string | null>(null);
  const [pending, setPending] = useState<PendingClip[]>([]);
  const [pendingLoading, setPendingLoading] = useState(true);
  const [posted, setPosted] = useState<PostedClip[]>([]);
  const [postedLoading, setPostedLoading] = useState(true);
  const [gifs, setGifs] = useState<StreamerGif[]>([]);
  const [gifsLoading, setGifsLoading] = useState(true);
  const [gifsIncludeHidden, setGifsIncludeHidden] = useState(false);
  const [view, setView] = useState<"main" | "posted" | "inspector" | "usersbots" | "gifs" | "roster">("main");
  const [usersBotsTarget, setUsersBotsTarget] = useState<UsersBotsTarget | null>(null);
  const [approvingAll, setApprovingAll] = useState(false);
  const [approveAllResult, setApproveAllResult] = useState<string | null>(null);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [triggerResult, setTriggerResult] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // The 30s poll effect runs once with [] deps, so its closure would freeze the
  // include_hidden flag at its initial value — read it from a ref instead.
  const gifsHiddenRef = useRef(false);

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

  const refreshPosted = async () => {
    try {
      const r = await api.streamersPublished();
      setPosted(r.published);
    } catch {} finally {
      setPostedLoading(false);
    }
  };

  const refreshGifs = async (includeHidden = gifsHiddenRef.current) => {
    try {
      const r = await api.streamersGifs(includeHidden);
      setGifs(r.gifs);
    } catch {} finally {
      setGifsLoading(false);
    }
  };

  const onGifReviewed = (clip_id: string, verdict: "good" | "hidden") => {
    setGifs((prev) =>
      gifsHiddenRef.current
        ? prev.map((g) => (g.clip_id === clip_id ? { ...g, verdict } : g))
        : prev.filter((g) => g.clip_id !== clip_id || verdict !== "hidden"),
    );
  };

  const onGifPosted = (_clip_id: string) => {
    // Keep the gif in the library — refresh so it re-renders with its tweet_url
    // (the "posted" badge) rather than disappearing.
    refreshGifs();
    refreshPosted();
  };

  const toggleGifsHidden = () => {
    const next = !gifsIncludeHidden;
    gifsHiddenRef.current = next;
    setGifsIncludeHidden(next);
    setGifsLoading(true);
    refreshGifs(next);
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

  const dismiss = (clipId: string) =>
    setDismissed((prev) => new Set(prev).add(clipId));

  const doTrigger = async (name: "FetchClips" | "PublishClip") => {
    setTriggering(name);
    setTriggerResult(null);
    try {
      await api.streamersTrigger(name);
      setTriggerResult(`Triggered ${name}.`);
      await refreshFlows();
    } catch (e) {
      setTriggerResult(`Error triggering ${name}: ${String(e)}`);
    } finally {
      setTriggering(null);
    }
  };

  const doApproveAll = async () => {
    if (visibleClips.length === 0) return;
    setApprovingAll(true);
    setApproveAllResult(null);
    let approved = 0;
    let failed = 0;
    for (const clip of visibleClips) {
      const text = clip.caption?.trim() || fallbackCaption();
      if (!clip.clip_path || !text.trim()) {
        failed++;
        continue;
      }
      try {
        await api.streamersApprove(
          clip.clip_path, text, clip.clip_id, clip.title,
          clip.source, clip.streamer, clip.url, clip.thumbnail_url, clip.x_handle,
          clip.view_count, clip.duration, clip.created_at,
        );
        approved++;
        dismiss(clip.clip_id ?? "");
      } catch {
        failed++;
      }
    }
    await refreshPending();
    setApproveAllResult(`Approved ${approved}${failed ? `, ${failed} failed` : ""}.`);
    setApprovingAll(false);
  };

  const onApproved = (clipId: string) => {
    dismiss(clipId);
    refreshPending();
  };

  const onReviewPostNow = (clipId: string) => {
    dismiss(clipId);
    refreshPosted();
  };

  const cancelPending = (clip_id: string) =>
    setPending((prev) => prev.filter((p) => p.clip_id !== clip_id));

  const onPendingPostNow = (clip_id: string) => {
    cancelPending(clip_id);
    refreshPosted();
  };

  useEffect(() => {
    refreshFlows();
    refreshQueue();
    refreshTopics();
    refreshPending();
    refreshPosted();
    refreshGifs();

    const startPoll = () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(() => {
        if (!document.hidden) {
          refreshFlows();
          refreshPending();
          refreshGifs();
        }
      }, 30000);
    };

    const onVisibility = () => {
      if (document.hidden) {
        if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      } else {
        refreshFlows();
        refreshPending();
        refreshGifs();
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

  const flowNames = ["FetchClips", "ProcessClips", "PublishClipOffPeakDay", "PublishClipPeakTimeCron"] as const;
  const visibleClips = clips.filter((c) => !dismissed.has(c.clip_id ?? ""));

  return (
    <div className="space-y-4">
      {/* ── Sub-nav pills ───────────────────────────────────────────── */}
      <div className="flex rounded-full border border-border overflow-hidden text-xs font-semibold w-fit">
        <button
          onClick={() => setView("main")}
          className={`px-3 py-1 transition-colors ${view === "main" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          Overview
        </button>
        <button
          onClick={() => setView("posted")}
          className={`px-3 py-1 border-l border-border transition-colors ${view === "posted" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          Posted Clips{posted.length > 0 ? ` (${posted.length})` : ""}
        </button>
        <button
          onClick={() => setView("inspector")}
          className={`px-3 py-1 border-l border-border transition-colors ${view === "inspector" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          Inspector
        </button>
        <button
          onClick={() => setView("usersbots")}
          className={`px-3 py-1 border-l border-border transition-colors ${view === "usersbots" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          Users/Bots
        </button>
        <button
          onClick={() => setView("gifs")}
          className={`px-3 py-1 border-l border-border transition-colors ${view === "gifs" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          GIFs{gifs.length > 0 ? ` (${gifs.length})` : ""}
        </button>
        <button
          onClick={() => setView("roster")}
          className={`px-3 py-1 border-l border-border transition-colors ${view === "roster" ? "bg-accent text-bg" : "bg-bg text-muted hover:text-text"}`}
        >
          Watchlist
        </button>
      </div>

      {view === "main" && (
      <>
      {/* ── Section 1: Pipeline Status ─────────────────────────────── */}
      <Card>
        <CardTitle>Pipeline Status</CardTitle>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
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
          <div className="flex items-center gap-2">
            <Button
              className="text-xs"
              onClick={() => doTrigger("FetchClips")}
              disabled={triggering !== null}
            >
              {triggering === "FetchClips" ? "Triggering…" : "Trigger FetchClips"}
            </Button>
            <Button
              className="text-xs"
              onClick={() => doTrigger("PublishClip")}
              disabled={triggering !== null}
            >
              {triggering === "PublishClip" ? "Triggering…" : "Trigger PublishClips"}
            </Button>
            <Button
              className="text-xs"
              onClick={doApproveAll}
              disabled={approvingAll || visibleClips.length === 0}
            >
              {approvingAll ? "Approving…" : "Approve All"}
            </Button>
            <Button className="text-xs" onClick={refreshQueue}>
              Refresh
            </Button>
          </div>
        </div>
        {triggerResult && (
          <p className={`text-xs mb-3 ${triggerResult.startsWith("Error") ? "text-bad" : "text-accent"}`}>{triggerResult}</p>
        )}
        {approveAllResult && (
          <p className="text-xs mb-3 text-accent">{approveAllResult}</p>
        )}
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
                key={clip.clip_id ?? i}
                clip={clip}
                onPublished={onApproved}
                onPostNow={onReviewPostNow}
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
        <PendingPanel pending={pending} loading={pendingLoading} onCancel={cancelPending} onPostedNow={onPendingPostNow} />
      </Card>
      </>
      )}

      {view === "posted" && (
      <Card>
        <div className="flex items-center justify-between mb-2">
          <CardTitle>
            Posted Clips
            {posted.length > 0 && (
              <span className="ml-2 text-xs text-muted font-normal">
                {posted.length} recent
              </span>
            )}
          </CardTitle>
          <Button className="text-xs" onClick={refreshPosted}>
            Refresh
          </Button>
        </div>
        <PostedClipsPanel posted={posted} loading={postedLoading} />
      </Card>
      )}

      {view === "inspector" && (
        <Inspector
          onOpenUsersBots={(platform, login) => {
            setUsersBotsTarget({ platform, login, nonce: Date.now() });
            setView("usersbots");
          }}
        />
      )}

      {view === "usersbots" && <UsersBots target={usersBotsTarget} />}

      {view === "gifs" && (
      <Card>
        <div className="flex items-center justify-between mb-2">
          <CardTitle>
            GIFs
            {gifs.length > 0 && (
              <span className="ml-2 text-xs text-muted font-normal">
                {gifs.length} cut
              </span>
            )}
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button className="text-xs" variant="ghost" onClick={toggleGifsHidden}>
              {gifsIncludeHidden ? "Hide hidden" : "Show hidden"}
            </Button>
            <Button className="text-xs" onClick={() => refreshGifs()}>
              Refresh
            </Button>
          </div>
        </div>
        <GifsPanel
          items={gifs}
          loading={gifsLoading}
          onReviewed={onGifReviewed}
          onPosted={onGifPosted}
        />
      </Card>
      )}

      {view === "roster" && <RosterGrid />}

    </div>
  );
}

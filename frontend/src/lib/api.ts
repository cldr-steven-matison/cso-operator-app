export type HealthService = {
  ok: boolean;
  status?: number;
  error?: string;
  topics?: number;
  // vLLM-specific: configured model and the list reported by /v1/models.
  configured?: string;
  loaded?: string[];
};
export type Health = {
  ok: boolean;
  // Partial: /api/health only includes keys for services owned by an active MODULES flag.
  services: Partial<Record<"vllm" | "qdrant" | "embedding" | "whisper" | "nifi" | "kafka" | "efm", HealthService>>;
};

export type EfmAgentClass = { name: string; agentCount: number };
export type EfmAgent = {
  identifier: string;
  className: string;
  lastSeen: string | null;
  status: Record<string, unknown>;
  endpointUrl: string;
};
export type EfmSendResult = { ok: boolean; status_code: number; body_preview: string };
export type EfmDemoExpect = { topic: string; withinSec: number; match?: string };
export type EfmDemo = {
  name: string;
  agentClass: string;
  contentType: string;
  payload: string;
  kafkaTopic: string;
  expect: EfmDemoExpect;
};

export type NifiPg = { id: string; version: number; state: string };
export type NifiState = Record<string, NifiPg>;

export type QdrantStats = {
  exists: boolean;
  points_count?: number;
  vectors_count?: number;
  segments_count?: number;
  status?: string;
};

export type KafkaTopic = { topic: string; exists: boolean; partitions?: number; depth?: number };
export type KafkaTopicsResponse = KafkaTopic[] | { error: string; topics: KafkaTopic[] };
export type KafkaAllTopic = { topic: string; partitions: number; depth: number };
export type KafkaAllTopicsResponse = KafkaAllTopic[] | { error: string; topics: KafkaAllTopic[] };

export type KafkaPeekMsg = {
  topic: string;
  partition: number;
  offset: number;
  ts: number | null;
  size: number;
  payload: string;
  payload_b64?: string;
};

export type Operator = {
  name: string;
  deployment: string;
  namespace: string;
  installed: boolean;
  ready: number;
  replicas: number;
  image: string;
  version: string;
  crd_groups: string[];
  crds_present: number;
  error?: string;
};

export type PodInfo = {
  name: string;
  phase: string;
  ready: number;
  containers: number;
  restarts: number;
  age_seconds: number;
  node: string;
  owner_kind: string;
  owner_name: string;
};

// ── Streamers module types ──────────────────────────────────────────────────

export type StreamerFlowState = { id: string | null; version: number; state: string };
export type StreamerFlows = Record<string, StreamerFlowState>;

export type StreamerClip = {
  clip_id?: string;
  source?: string;
  streamer?: string;
  x_handle?: string;
  title?: string;
  url?: string;
  thumbnail_url?: string;
  duration?: number;
  view_count?: number;
  created_at?: string;
  clip_path?: string;
  gif_path?: string;
  paths?: { clip: boolean; gif: boolean; gif_post?: boolean };
  transcript?: string;
  caption?: string;
  // "brain" = the Spark 35B wrote the posted caption (#272 B5);
  // "reaction"/"quoted" = the 3B fallback path produced it.
  caption_mode?: string;
  // The DGX Spark brain's raw caption + its self-check JSON (#277). When
  // caption_mode is "brain", `caption` above is this plus emoji rule + suffix.
  brain_caption?: string;
  brain?: Record<string, unknown>;
  _offset?: number;
  _partition?: number;
  _ts?: number;
};

export type StreamerPublishResult = { ok: boolean; tweet_id: string; url: string };
export type WatchlistResponse = { logins: string[] };

// One row of the Postgres `streamer` table (roster_store.Streamer.as_dict) — #279.
export type RosterRow = {
  platform: "twitch" | "kick";
  login: string;
  entry: string; // login | kick:login
  x_handle: string;
  x_handle_status: "" | "confirmed" | "needs_review";
  clip_enabled: boolean;
  gif_enabled: boolean;
  gif_post_enabled: boolean;
  active: boolean;
  added_by: string;
  added_at: string;
  updated_at: string;
  source: string;
  display_name: string;
  aliases: string[];
  pronouns: string;
  pronouns_status: "" | "confirmed" | "needs_review";
  notes: string;
};
export type RosterPatch = Partial<
  Pick<
    RosterRow,
    | "x_handle" | "x_handle_status" | "clip_enabled" | "gif_enabled" | "gif_post_enabled"
    | "active" | "display_name" | "aliases" | "pronouns" | "pronouns_status" | "notes"
  >
>;
export type RosterResult = { ok: boolean; login?: string; reason?: string; message?: string; row?: RosterRow };

export type PendingClip = {
  clip_id: string;
  clip_path: string;
  gif_path?: string;
  paths?: { clip: boolean; gif: boolean; gif_post?: boolean };
  tweet_text: string;
  title?: string;
  source?: string;
  streamer?: string;
  url?: string;
  thumbnail_url?: string;
  x_handle?: string;
  view_count?: number;
  duration?: number;
  created_at?: string;
};

export type StreamerGif = {
  clip_id: string;
  streamer: string;
  source: string;
  title: string;
  url: string;
  thumbnail_url: string;
  x_handle: string;
  view_count: number;
  created_at: string;
  gif_path: string;
  gif_bytes: number;
  crop_why: string;
  gif_error: string;
  indexed_at: string;
  verdict: "good" | "hidden" | null;
  // Set once the gif has been posted to X. Posting does NOT remove it from the
  // library listing — only a "hidden" verdict does.
  tweet_url?: string;
  tweet_id?: string;
  posted_at?: string;
};

export type InspectorClip = {
  clip_id?: string;
  title?: string;
  duration?: number;
  view_count?: number;
  created_at?: string;
  thumbnail_url?: string;
  url?: string;
};

export type InspectorChatter = {
  username: string;
  message_count: number;
  badges: string[];
  investment_score: number;
  samples: string[];
  is_bot: boolean;
  // Only present on chat-activity snapshots (WatchlistChatSnapshotPoller) —
  // how many *other* watchlisted channels this username has recently shown
  // up as an active chatter in. Absent (not zero) on one-shot /inspect/chat
  // results, which have no cross-channel visibility.
  cross_channel_count?: number;
};

export type MessageCluster = {
  sample_text: string;
  distinct_senders: number;
  total_messages: number;
  senders: string[];
};

export type InspectorAltPlatform = {
  platform: "twitch" | "kick";
  exists: boolean | null;
  live?: boolean;
  sample_clips?: InspectorClip[];
  error?: string;
};

export type InspectorResult = {
  login: string;
  platform: "twitch" | "kick";
  live: boolean;
  clips: InspectorClip[];
  alt_platform: InspectorAltPlatform;
};

export type ChatInspectResult = {
  login: string;
  platform: "twitch" | "kick";
  live: boolean;
  viewer_count: number | null;
  duration_sec: number;
  unique_chatters: number;
  messages_seen: number;
  message_cap_hit: boolean;
  bots: InspectorChatter[];
  chatters: InspectorChatter[];
  clusters: MessageCluster[];
  // unique_chatters vs viewer_count heuristic — see inspector.py's threshold
  // constants. null/false when viewer_count is unavailable.
  engagement_ratio: number | null;
  bot_flag_likely: boolean;
  error?: string | null;
  note?: string | null;
};

// Same shape as ChatInspectResult (WatchlistChatSnapshotPoller re-runs
// /inspect/chat verbatim and publishes the result) plus a short recent
// history — for the Users/Bots page's live mode on watchlisted channels.
export type ChatActivitySnapshot = ChatInspectResult & {
  history: ChatInspectResult[];
};

export type PostedClip = {
  clip_id: string;
  title?: string;
  source?: string;
  streamer?: string;
  url?: string;
  thumbnail_url?: string;
  x_handle?: string;
  tweet_id?: string;
  tweet_url?: string;
  published_at?: string;
};

export type TopicRecord = {
  offset: number;
  source?: string;
  streamer: string;
  title: string;
  clip_id: string;
  caption: string;
  has_file: boolean;
};
export type TopicStats = {
  count: number;
  records: TopicRecord[];
  error?: string;
};
export type StreamerTopics = {
  new_clips: TopicStats;
  processed_clips: TopicStats;
};
export type KafkaResetResult = {
  deleted_topics: string[];
  removed_clips: number;
  seen_clips_reset: boolean;
  errors: string[];
};

export type PodSummary = {
  ns: string;
  total: number;
  running: number;
  pending: number;
  failed: number;
  succeeded: number;
  pods: PodInfo[];
  error?: string;
};

async function jget<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

async function jpost<T>(url: string, body?: unknown): Promise<T> {
  const r = await fetch(url, {
    method: "POST",
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

export const api = {
  health: () => jget<Health>("/api/health"),

  nifiState: () => jget<NifiState>("/api/nifi/state"),
  nifiStart: (name: string) => jpost(`/api/nifi/${encodeURIComponent(name)}/start`),
  nifiStop: (name: string) => jpost(`/api/nifi/${encodeURIComponent(name)}/stop`),

  qdrantStats: () => jget<QdrantStats>("/api/qdrant/stats"),
  qdrantRecreate: () => jpost("/api/qdrant/recreate"),

  kafkaTopics: () => jget<KafkaTopicsResponse>("/api/kafka/topics"),
  kafkaAllTopics: () => jget<KafkaAllTopicsResponse>("/api/kafka/all-topics"),
  kafkaPeek: (topic: string, limit = 10) =>
    jget<KafkaPeekMsg[]>(`/api/kafka/peek/${encodeURIComponent(topic)}?limit=${limit}`),

  k8sOperators: () => jget<Operator[]>("/api/k8s/operators"),
  k8sPods: () => jget<PodSummary[]>("/api/k8s/pods"),
  k8sRestart: (ns: string, name: string) =>
    jpost(`/api/k8s/deploy/${encodeURIComponent(ns)}/${encodeURIComponent(name)}/restart`),
  k8sDeletePod: (ns: string, name: string) =>
    fetch(`/api/k8s/pod/${encodeURIComponent(ns)}/${encodeURIComponent(name)}`, {
      method: "DELETE",
    }).then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    }),

  efmAgentClasses: () => jget<EfmAgentClass[]>("/api/efm/agent-classes"),
  efmAgents: () => jget<EfmAgent[]>("/api/efm/agents"),
  efmDemos: () => jget<EfmDemo[]>("/api/efm/demos"),
  efmSend: (endpointUrl: string, payload: string, contentType: string) =>
    jpost<EfmSendResult>("/api/efm/send", { endpoint_url: endpointUrl, payload, content_type: contentType }),

  ingest: (file: File) => uploadFile("/api/ingest", file),
  sampleAudioUrl: "/api/sample-audio",

  // Streamers module
  streamersFlows: () => jget<StreamerFlows>("/api/streamers/flows"),
  streamersFlowStart: (name: string) => jpost(`/api/streamers/flows/${encodeURIComponent(name)}/start`),
  streamersFlowStop: (name: string) => jpost(`/api/streamers/flows/${encodeURIComponent(name)}/stop`),
  streamersTrigger: (name: string) => jpost<{ ok: boolean; request: string; status: number }>(`/api/streamers/flows/trigger/${encodeURIComponent(name)}`),
  streamersQueue: () => jget<StreamerClip[]>("/api/streamers/queue"),
  streamersApprove: (
    clip_path: string, tweet_text: string, clip_id?: string, title?: string,
    source?: string, streamer?: string, url?: string, thumbnail_url?: string, x_handle?: string,
    view_count?: number, duration?: number, created_at?: string,
  ) =>
    jpost<{ queued: boolean; clip_id: string; position: number }>("/api/streamers/approve", {
      clip_path, tweet_text, clip_id, title, source, streamer, url, thumbnail_url, x_handle, view_count,
      duration, created_at,
    }),
  streamersPublish: (
    clip_path: string, tweet_text: string, clip_id?: string, title?: string,
    source?: string, streamer?: string, url?: string, thumbnail_url?: string, x_handle?: string,
  ) =>
    jpost<StreamerPublishResult>("/api/streamers/publish", {
      clip_path, tweet_text, clip_id, title, source, streamer, url, thumbnail_url, x_handle,
    }),
  streamersSkip: (clip_id: string) =>
    jpost<{ ok: boolean; clip_id: string }>("/api/streamers/skip", { clip_id }),
  streamersWatchlist: () => jget<WatchlistResponse>("/api/streamers/watchlist"),
  streamersSetWatchlist: (logins: string[]) =>
    jpost<WatchlistResponse>("/api/streamers/watchlist", { logins }),
  streamersRotateWatchlist: () =>
    jpost<WatchlistResponse>("/api/streamers/watchlist/rotate", {}),
  streamersWatchlistAdd: (login: string, platform: "twitch" | "kick") =>
    jpost<WatchlistResponse>("/api/streamers/watchlist/add", { login, platform }),
  streamersWatchlistRemove: (login: string, platform: "twitch" | "kick") =>
    jpost<WatchlistResponse>("/api/streamers/watchlist/remove", { login, platform }),
  // Roster grid (#279)
  streamersRosterRows: () =>
    jget<{ ok: boolean; reason?: string; rows: RosterRow[]; watchlist: string[] }>(
      "/api/streamers/roster/rows",
    ),
  streamersRosterAdd: (login: string, platform: "twitch" | "kick") =>
    jpost<RosterResult>("/api/streamers/roster/add", { login, platform }),
  streamersRosterUpdate: (platform: string, login: string, patch: RosterPatch) =>
    fetch(`/api/streamers/roster/${encodeURIComponent(platform)}/${encodeURIComponent(login)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    }).then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json() as Promise<RosterResult>;
    }),
  streamersRosterDelete: (platform: string, login: string, hard = false) =>
    fetch(
      `/api/streamers/roster/${encodeURIComponent(platform)}/${encodeURIComponent(login)}${hard ? "?hard=true" : ""}`,
      { method: "DELETE" },
    ).then((r) => {
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json() as Promise<{ ok: boolean; login: string; removed: boolean; hard: boolean }>;
    }),
  streamersTopics: () => jget<StreamerTopics>("/api/streamers/topics"),
  streamersReset: () => jpost<KafkaResetResult>("/api/streamers/reset"),
  streamersFetchMode: () => jget<{ mode: string; period: string }>("/api/streamers/fetch-mode"),
  streamersSetFetchMode: (mode: string, period: string) =>
    jpost<{ mode: string; period: string }>("/api/streamers/fetch-mode", { mode, period }),
  streamersPending: () => jget<{ pending: PendingClip[] }>("/api/streamers/pending"),
  streamersCancelPending: (clip_id: string) =>
    jpost<{ ok: boolean; clip_id: string }>(`/api/streamers/pending/${encodeURIComponent(clip_id)}/cancel`),
  streamersPendingPublishNow: (clip_id: string) =>
    jpost<{ published: boolean; ok?: boolean; url?: string; queue_remaining?: number; reason?: string }>(
      `/api/streamers/pending/${encodeURIComponent(clip_id)}/publish-now`,
    ),
  streamersPublished: () => jget<{ published: PostedClip[] }>("/api/streamers/published"),
  streamersInspect: (login: string, clipLimit = 12) =>
    jget<InspectorResult>(
      `/api/streamers/inspect?login=${encodeURIComponent(login)}&clip_limit=${clipLimit}`,
    ),
  streamersInspectChat: (login: string, chatSeconds = 25) =>
    jget<ChatInspectResult>(
      `/api/streamers/inspect/chat?login=${encodeURIComponent(login)}&chat_seconds=${chatSeconds}`,
    ),
  streamersChatActivity: (login: string) =>
    jget<ChatActivitySnapshot>(`/api/streamers/chat-activity/${encodeURIComponent(login)}`),
  streamersLiveBulk: (logins: string[]) =>
    jget<{ statuses: Record<string, boolean> }>(
      `/api/streamers/live-bulk?logins=${encodeURIComponent(logins.join(","))}`,
    ),
  streamersLiveNow: () => jget<{ live: string[] }>("/api/streamers/live-now"),
  streamersQueueClip: (
    platform: "twitch" | "kick",
    streamer: string,
    clip_id: string,
    url: string,
    thumbnail_url: string,
    title: string,
    view_count: number,
    created_at: string,
  ) =>
    jpost<{ ok: boolean; clip_id?: string; duration?: number; error?: string }>(
      "/api/streamers/inspect/queue-clip",
      { platform, streamer, clip_id, url, thumbnail_url, title, view_count, created_at },
    ),
  streamersGifs: (includeHidden = false) =>
    jget<{ gifs: StreamerGif[] }>(
      `/api/streamers/gifs${includeHidden ? "?include_hidden=1" : ""}`,
    ),
  streamersGifReview: (clip_id: string, verdict: "good" | "hidden") =>
    jpost<{ ok: boolean; clip_id: string; verdict: string }>(
      `/api/streamers/gifs/${encodeURIComponent(clip_id)}/review`,
      { verdict },
    ),
  streamersGifPostNow: (clip_id: string) =>
    jpost<{ ok?: boolean; published?: boolean; tweet_id?: string; url?: string; reason?: string }>(
      `/api/streamers/gifs/${encodeURIComponent(clip_id)}/post-now`,
    ),
};

async function uploadFile(url: string, file: File) {
  const form = new FormData();
  form.append("file", file);
  const r = await fetch(url, { method: "POST", body: form });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

/** Open an SSE stream. `onEvent(name, data)` for any named event; default name is "message". */
export function openSSE(
  url: string,
  onEvent: (name: string, data: string) => void,
  init?: RequestInit & { body?: BodyInit }
): () => void {
  const ctrl = new AbortController();
  (async () => {
    const r = await fetch(url, { ...init, signal: ctrl.signal });
    if (!r.body) return;
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const block = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        let event = "message";
        let data = "";
        for (const line of block.split("\n")) {
          if (line.startsWith("event: ")) event = line.slice(7).trim();
          else if (line.startsWith("data: ")) data += line.slice(6);
        }
        if (data) onEvent(event, data);
      }
    }
  })().catch(() => {});
  return () => ctrl.abort();
}

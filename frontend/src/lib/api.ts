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
  transcript?: string;
  caption?: string;
  _offset?: number;
  _partition?: number;
  _ts?: number;
};

export type StreamerPublishResult = { ok: boolean; tweet_id: string; url: string };
export type WatchlistResponse = { logins: string[] };

export type PendingClip = {
  clip_id: string;
  clip_path: string;
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

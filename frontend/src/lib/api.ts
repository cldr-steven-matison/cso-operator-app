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
  services: Record<"vllm" | "qdrant" | "embedding" | "whisper" | "nifi" | "kafka" | "efm", HealthService>;
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
  efmSend: (endpointUrl: string, payload: string, contentType: string) =>
    jpost<EfmSendResult>("/api/efm/send", { endpoint_url: endpointUrl, payload, content_type: contentType }),

  ingest: (file: File) => uploadFile("/api/ingest", file),
  sampleAudioUrl: "/api/sample-audio",
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

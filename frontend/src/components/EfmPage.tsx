import { useEffect, useRef, useState } from "react";

import { Badge, Dot } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import {
  api,
  type EfmAgent,
  type EfmAgentClass,
  type EfmSendResult,
  type KafkaAllTopic,
  type KafkaAllTopicsResponse,
  type KafkaPeekMsg,
} from "@/lib/api";

type Tone = "ok" | "warn" | "bad" | "neutral";

// ─── helpers ───────────────────────────────────────────────────────────────

function relTime(ts: string | null): string {
  if (!ts) return "never";
  const diff = (Date.now() - new Date(ts).getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

function agentTone(agent: EfmAgent): Tone {
  if (!agent.lastSeen) return "neutral";
  const diff = (Date.now() - new Date(agent.lastSeen).getTime()) / 1000;
  if (diff <= 30) return "ok";
  if (diff <= 300) return "warn";
  return "bad";
}

function asAllTopics(r: KafkaAllTopicsResponse): KafkaAllTopic[] {
  return Array.isArray(r) ? r : r.topics;
}

// ─── component ─────────────────────────────────────────────────────────────

export function EfmPage() {
  // ── section 1 & 2 state ────────────────────────────────────────────────
  const [classes, setClasses] = useState<EfmAgentClass[]>([]);
  const [classesLoading, setClassesLoading] = useState(true);
  const [agents, setAgents] = useState<EfmAgent[]>([]);

  useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const [cls, ags] = await Promise.all([api.efmAgentClasses(), api.efmAgents()]);
        if (!alive) return;
        setClasses(cls);
        setClassesLoading(false);
        setAgents(ags);
      } catch {
        if (alive) setClassesLoading(false);
      }
    };
    refresh();
    const id = setInterval(refresh, 15000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  // ── section 3 state ────────────────────────────────────────────────────
  const [selectedAgentId, setSelectedAgentId] = useState<string>("");
  const [endpointUrl, setEndpointUrl] = useState<string>("");
  const [contentType, setContentType] = useState<string>("application/json");
  const [payload, setPayload] = useState<string>("{}");
  const [sending, setSending] = useState(false);
  const [sendResult, setSendResult] = useState<EfmSendResult | null>(null);

  const [peekTopic, setPeekTopic] = useState<string>("new_documents");
  const [allTopics, setAllTopics] = useState<KafkaAllTopic[]>([]);
  const [peekedMsgs, setPeekedMsgs] = useState<KafkaPeekMsg[]>([]);
  const [peeking, setPeeking] = useState(false);
  const peekIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch all Kafka topics once on mount
  useEffect(() => {
    api.kafkaAllTopics().then((r) => setAllTopics(asAllTopics(r))).catch(() => {});
  }, []);

  // Auto-fill endpointUrl when selectedAgentId changes
  useEffect(() => {
    if (!selectedAgentId) return;
    const agent = agents.find((a) => a.identifier === selectedAgentId);
    if (agent) setEndpointUrl(agent.endpointUrl);
  }, [selectedAgentId, agents]);

  // Default to first agent when agents load
  useEffect(() => {
    if (agents.length > 0 && !selectedAgentId) {
      setSelectedAgentId(agents[0].identifier);
      setEndpointUrl(agents[0].endpointUrl);
    }
  }, [agents, selectedAgentId]);

  // Cleanup peek interval on unmount
  useEffect(() => {
    return () => {
      if (peekIntervalRef.current !== null) {
        clearInterval(peekIntervalRef.current);
      }
    };
  }, []);

  function startPeekAutoRefresh() {
    if (peekIntervalRef.current !== null) return; // already running
    setPeeking(true);
    peekIntervalRef.current = setInterval(async () => {
      try {
        const msgs = await api.kafkaPeek(peekTopic, 5);
        setPeekedMsgs(msgs);
      } catch {}
    }, 5000);
  }

  async function doManualPeek() {
    try {
      const msgs = await api.kafkaPeek(peekTopic, 5);
      setPeekedMsgs(msgs);
    } catch {}
  }

  async function doSend() {
    if (!endpointUrl) return;
    setSending(true);
    setSendResult(null);
    try {
      const result = await api.efmSend(endpointUrl, payload, contentType);
      setSendResult(result);
      if (result.ok) {
        // Initial peek then start auto-refresh
        try {
          const msgs = await api.kafkaPeek(peekTopic, 5);
          setPeekedMsgs(msgs);
        } catch {}
        startPeekAutoRefresh();
      }
    } catch (e) {
      setSendResult({ ok: false, status_code: 0, body_preview: String(e) });
    } finally {
      setSending(false);
    }
  }

  // When peekTopic changes, stop auto-refresh so the user can restart cleanly
  function handlePeekTopicChange(topic: string) {
    setPeekTopic(topic);
    if (peekIntervalRef.current !== null) {
      clearInterval(peekIntervalRef.current);
      peekIntervalRef.current = null;
      setPeeking(false);
    }
    setPeekedMsgs([]);
  }

  // ── render ─────────────────────────────────────────────────────────────
  return (
    <div className="space-y-4">
      {/* ── Section 1: Agent Classes ─────────────────────────────────── */}
      <Card>
        <CardTitle>Agent Classes</CardTitle>
        {classesLoading ? (
          <p className="text-muted text-sm">Loading...</p>
        ) : classes.length === 0 ? (
          <p className="text-muted text-sm">No agent classes found</p>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {classes.map((cls) => (
              <div key={cls.name} className="border border-border rounded p-3 bg-bg">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-mono text-text">{cls.name}</span>
                  <Badge tone={cls.agentCount > 0 ? "ok" : "neutral"}>
                    {cls.agentCount} agent{cls.agentCount !== 1 ? "s" : ""}
                  </Badge>
                </div>
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* ── Section 2: Active Agents ─────────────────────────────────── */}
      <Card>
        <CardTitle>Active Agents</CardTitle>
        {agents.length === 0 ? (
          <p className="text-muted text-sm">No active agents</p>
        ) : (
          <div className="space-y-2">
            {agents.map((agent) => (
              <div
                key={agent.identifier}
                className="flex items-center gap-3 border border-border rounded p-2 bg-bg"
              >
                <Dot tone={agentTone(agent)} />
                <span className="font-mono text-xs text-text">
                  {agent.identifier.slice(0, 8)}…
                </span>
                <span className="text-xs text-muted">{agent.className}</span>
                <span className="text-xs text-muted ml-auto">{relTime(agent.lastSeen)}</span>
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* ── Section 3: Test Agent ────────────────────────────────────── */}
      <Card>
        <CardTitle>Test Agent</CardTitle>
        <div className="space-y-3">
          {/* Row 1: Agent selector */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted w-20 shrink-0">Agent:</span>
            <select
              value={selectedAgentId}
              onChange={(e) => setSelectedAgentId(e.target.value)}
              className="flex-1 bg-bg border border-border rounded px-2 py-1 text-sm text-text"
            >
              {agents.length === 0 && (
                <option value="">No agents available</option>
              )}
              {agents.map((a) => (
                <option key={a.identifier} value={a.identifier}>
                  {a.className} / {a.identifier.slice(0, 8)}
                </option>
              ))}
            </select>
          </div>

          {/* Row 2: Endpoint URL */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted w-20 shrink-0">Endpoint:</span>
            <input
              type="text"
              value={endpointUrl}
              onChange={(e) => setEndpointUrl(e.target.value)}
              className="flex-1 bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text"
            />
          </div>

          {/* Row 3: Content-Type */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted w-20 shrink-0">Type:</span>
            <select
              value={contentType}
              onChange={(e) => setContentType(e.target.value)}
              className="bg-bg border border-border rounded px-2 py-1 text-sm text-text"
            >
              <option value="application/json">application/json</option>
              <option value="text/plain">text/plain</option>
            </select>
          </div>

          {/* Row 4: Payload */}
          <textarea
            rows={6}
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            className="w-full bg-bg border border-border rounded px-2 py-1 text-xs font-mono text-text resize-y"
          />

          {/* Row 5: Send button + result */}
          <div className="flex items-center gap-3 flex-wrap">
            <Button onClick={doSend} disabled={sending || !endpointUrl}>
              {sending ? "Sending…" : "Send to Agent"}
            </Button>
            {sendResult && (
              <span className={sendResult.ok ? "text-accent text-xs" : "text-bad text-xs"}>
                {sendResult.status_code} {sendResult.ok ? "OK" : "ERR"} —{" "}
                {sendResult.body_preview.slice(0, 80)}
              </span>
            )}
          </div>

          {/* Row 6: Kafka Response panel */}
          <div className="border border-border rounded p-3 bg-bg space-y-2">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs text-muted">Kafka topic:</span>
              <select
                value={peekTopic}
                onChange={(e) => handlePeekTopicChange(e.target.value)}
                className="bg-bg border border-border rounded px-2 py-1 text-sm text-text"
              >
                {allTopics.length === 0 && (
                  <option value={peekTopic}>{peekTopic}</option>
                )}
                {allTopics.map((t) => (
                  <option key={t.topic} value={t.topic}>
                    {t.topic}
                  </option>
                ))}
              </select>
              <Button
                className="px-2 py-0.5 text-xs"
                onClick={doManualPeek}
              >
                Peek
              </Button>
              {peeking && (
                <span className="text-xs text-muted animate-pulse">auto-refresh</span>
              )}
            </div>
            {peekedMsgs.map((m) => (
              <div
                key={m.offset}
                className="text-xs font-mono text-muted border-t border-border pt-1"
              >
                <span>offset {m.offset}</span>
                {" | "}
                <span>{m.ts ? new Date(m.ts).toLocaleTimeString() : "—"}</span>
                {" | "}
                <span className="text-text">{m.payload.slice(0, 120)}</span>
              </div>
            ))}
            {peekedMsgs.length === 0 && (
              <p className="text-xs text-muted">No messages yet</p>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
}

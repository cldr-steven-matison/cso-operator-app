import { useEffect, useRef, useState } from "react";

import { Badge, Dot } from "@/components/ui/Badge";
import { Button } from "@/components/ui/Button";
import { Card, CardTitle } from "@/components/ui/Card";
import {
  api,
  type EfmAgent,
  type EfmAgentClass,
  type EfmDemo,
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

  const [demos, setDemos] = useState<EfmDemo[]>([]);
  const [selectedDemoName, setSelectedDemoName] = useState<string>("");
  // Captured at send time; the verdict watcher reads from this snapshot so
  // changing the demo selector mid-watch can't poison an in-flight result.
  const [activeExpect, setActiveExpect] = useState<
    { topic: string; match?: string; deadline: number; sentAt: number } | null
  >(null);
  const [verdict, setVerdict] = useState<"pending" | "pass" | "fail" | null>(null);

  const [peekTopic, setPeekTopic] = useState<string>("new_documents");
  const [allTopics, setAllTopics] = useState<KafkaAllTopic[]>([]);
  const [peekedMsgs, setPeekedMsgs] = useState<KafkaPeekMsg[]>([]);
  const [peeking, setPeeking] = useState(false);
  const peekIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch all Kafka topics + demo catalog once on mount
  useEffect(() => {
    api.kafkaAllTopics().then((r) => setAllTopics(asAllTopics(r))).catch(() => {});
    api.efmDemos().then(setDemos).catch(() => {});
  }, []);

  // Demos available for the currently-selected agent's class.
  const selectedAgent = agents.find((a) => a.identifier === selectedAgentId);
  const availableDemos = selectedAgent
    ? demos.filter((d) => d.agentClass === selectedAgent.className)
    : [];

  // Apply a demo: fill content-type, payload, peek topic; stash expectation
  // for the next send (NOT immediately — only after a successful send does
  // the watcher start).
  function applyDemo(name: string) {
    setSelectedDemoName(name);
    const d = demos.find((x) => x.name === name);
    if (!d) return;
    setContentType(d.contentType);
    setPayload(d.payload);
    handlePeekTopicChange(d.kafkaTopic);
    setVerdict(null);
    setActiveExpect(null);
  }

  // Match a peek message against the current expectation. A message counts
  // only if it (a) arrived after we sent, and (b) — when a `match` substring
  // is set — the payload contains it.
  function matchesExpect(
    m: KafkaPeekMsg,
    exp: { match?: string; sentAt: number },
  ): boolean {
    // If the broker reports a timestamp, require it to be after the send.
    // Missing ts falls back to substring-only (best-effort) so we don't
    // false-negative when the peek endpoint omits timestamps.
    if (m.ts) {
      const t = new Date(m.ts).getTime();
      if (!Number.isNaN(t) && t < exp.sentAt) return false;
    }
    if (exp.match && !m.payload.includes(exp.match)) return false;
    return true;
  }

  // Watch peek messages for the active expectation and PASS as soon as a
  // matching one lands. Deadline-driven FAIL lives in the effect below; we
  // never call setVerdict("fail") from here, because the peek auto-refresh
  // cadence (5 s) is coarser than the 1 s deadline tick — a message can
  // arrive between refreshes and get miscounted as FAIL.
  useEffect(() => {
    if (!activeExpect || verdict !== "pending") return;
    if (peekedMsgs.some((m) => matchesExpect(m, activeExpect))) {
      setVerdict("pass");
    }
  }, [peekedMsgs, activeExpect, verdict]);

  // Deadline enforcement. On tick, if we've past the deadline, do ONE final
  // synchronous peek before declaring FAIL — this closes the window where
  // Kafka has the message but our last cached peek doesn't. If the final
  // peek still doesn't contain a match, it's a real FAIL.
  useEffect(() => {
    if (verdict !== "pending" || !activeExpect) return;
    const exp = activeExpect;
    const id = setInterval(async () => {
      if (Date.now() < exp.deadline) return;
      clearInterval(id);
      try {
        const msgs = await api.kafkaPeek(exp.topic, 5);
        setPeekedMsgs(msgs);
        if (msgs.some((m) => matchesExpect(m, exp))) {
          setVerdict("pass");
          return;
        }
      } catch {
        // Fall through to FAIL — if we can't verify, we can't PASS.
      }
      setVerdict("fail");
    }, 1000);
    return () => clearInterval(id);
  }, [verdict, activeExpect]);

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
    // Reset prior verdict before this send. If a demo is selected, snapshot
    // its expectation against a fresh deadline so the watcher starts clean.
    setVerdict(null);
    setActiveExpect(null);
    try {
      // Capture sentAt just before the network call so the timestamp filter
      // is a small underestimate — better to accept a message than reject
      // one that raced ahead of our clock.
      const sentAt = Date.now();
      const result = await api.efmSend(endpointUrl, payload, contentType);
      setSendResult(result);
      if (result.ok) {
        const demo = demos.find((d) => d.name === selectedDemoName);
        if (demo) {
          setActiveExpect({
            topic: demo.expect.topic,
            match: demo.expect.match,
            deadline: sentAt + demo.expect.withinSec * 1000,
            sentAt,
          });
          setVerdict("pending");
        }
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

          {/* Row 1b: Demo preset (filtered by selected agent's class) */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted w-20 shrink-0">Demo:</span>
            <select
              value={selectedDemoName}
              onChange={(e) => applyDemo(e.target.value)}
              className="flex-1 bg-bg border border-border rounded px-2 py-1 text-sm text-text"
              disabled={availableDemos.length === 0}
            >
              <option value="">
                {availableDemos.length === 0 ? "(no demos for this agent class)" : "— select demo —"}
              </option>
              {availableDemos.map((d) => (
                <option key={d.name} value={d.name}>
                  {d.name}
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
            {verdict && (
              <Badge
                tone={
                  verdict === "pass" ? "ok" : verdict === "fail" ? "bad" : "warn"
                }
              >
                {verdict === "pending" ? "verifying…" : verdict.toUpperCase()}
              </Badge>
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

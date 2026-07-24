# CSO Operator App

![CSO Operator App Control Plane](/CSO_Operator_Control_Plane.png)

A control panel for the **Cloudera Streaming Operators** stack on Minikube — started as a RAG + Audio Transcription demo, has since grown a live streamer-clip-to-X pipeline module. See [Modules](#modules) below; the two are quite different in nature (one's a demo, the other posts real content to a real X account).

One screen drives, depending on which [modules](#modules) are enabled:

- **Documents and audio** both ingested through the same NiFi `IngestDataToStream` PG — a single `ListenHTTP` at the head, `RouteOnAttribute` branches on the upload's Content-Type: docs → Kafka `new_documents` → chunked + embedded + upserted into Qdrant by `StreamTovLLM`; audio → Kafka `new_audio` → transcribed by `StreamToWhisper` (insanely-fast-whisper, GPU) → republished to `new_documents` → indexed by the same RAG flow. (There is no separate `IngestDocsToStream` PG — confirmed against the live flow — despite what older docs/READMEs may say.)
- **Streaming RAG queries** against vLLM (Qwen2.5-3B-Instruct), with sources from Qdrant.
- **NiFi flow controls** (start/stop, live state).
- **Kafka topic activity** (depth, lag, live tail).
- **Qdrant collection management** (recreate, stats).
- **EFM agent control** — list `agent-classes`, list live agents (heartbeat
  staleness dots), and a Test Agent panel that POSTs to a MiNiFi agent's
  `ListenHTTP /contentListener` from a repo-local demo catalog
  (`samples/efm-demos.json`). The catalog is per-`agentClass`; each entry
  declares a Kafka topic + `expect` block, so the panel verifies
  end-to-end and shows a `PASS / FAIL` badge.
- **Streamers** — fetches Twitch/Kick clips on a watch list, transcribes (Whisper) and captions (vLLM) them, queues them for review, and publishes approved ones to X with commentary. Also runs `LiveStreamerAlert` (NiFi-native, posts "streamer is live" to X/Twitch chat) and a watchlist chat-join bot. Real X/Twitch/Kick credentials, real posts — not a demo. Full detail: [DesktopShare/cso-operator-app-streamers.md](https://github.com/cldr-steven-matison/DesktopShare/blob/main/cso-operator-app-streamers.md).

The Operator/RAG/EFM pieces are a local demo only — no auth, no production hardening. The Streamers module is real: real accounts, real posts, real credentials injected via `kubectl set env` (never in YAML/ConfigMaps).

## Modules

`MODULES` is a build-time + deploy-time flag controlling which optional tabs/routes are active. **Always state it explicitly on every `make deploy`/`make build`/`scripts/deploy.sh` call — there is no "correct" default, and a bare `make deploy` silently builds Operator-only, which has caused a real overnight outage before.**

| Value | Adds |
|---|---|
| *(empty)* | Operator tab only (pod/operator health) — this is what you get if you forget to pass `MODULES` |
| `rag` | RAG tab (document/audio ingest demo, NiFi controls, Kafka activity, Qdrant, RAG query) |
| `efm` | EFM tab (agent-class/agent list, Test Agent panel) |
| `streamers` | Streamers tab (see above) |

Combine with commas, e.g. `MODULES=rag,streamers,efm` for everything. Operator is always present regardless of `MODULES`.

```bash
make deploy MODULES=rag,streamers,efm     # full install
make deploy MODULES=streamers             # Streamers only, smallest live-posting install
make deploy MODULES=                      # Operator only, explicit bare minimum
```

**Two gotchas, both real:**
- **`rag` and `efm` only gate the frontend tab** — `backend/main.py` always registers the `nifi`/`qdrant`/`kafka`/`ingest`/`efm` routers no matter what `MODULES` says. `streamers` is the only value that actually changes backend behavior (`backend/routers/streamers.py` is registered only when `"streamers"` is in `MODULES`). So hiding the RAG/EFM tabs doesn't reduce the backend's surface area — only dropping `streamers` does.
- **`VITE_MODULES=all` is a real frontend shorthand for "show every tab" (`frontend/src/App.tsx`) — the backend has no equivalent.** Deploying with `MODULES=all` shows the Streamers tab in the UI but the backend never registers `/api/streamers/*` (it checks for the literal string `"streamers"`, not `"all"`) — every Streamers API call would 404. Don't use `all` for anything beyond local frontend-only experiments; spell out the real module list for a real deploy.

**First-time Streamers setup is a separate step, not part of `make bootstrap`.** `MODULES=streamers` gets you the tab and the backend routes, but the NiFi `StreamersApp` process group itself has to exist already — `scripts/setup-streamers-flows.py` creates it via the NiFi REST API (see the script's own docstring for exact invocation). On this deployment it already exists and has grown well past what that script creates (it only describes `FetchClips`/`ProcessClips`/`PublishClip` — the live flow also has `LiveStreamerAlert`, `TunaStarLinkFlows`, `WatchlistChatJoiner`, and a shared `Trigger`/`RouteOnAttribute` on-demand entry point; see the streamers doc linked above for the current real shape). On a genuinely fresh cluster, treat that script as a starting point, not the finished flow.

## Sources

- Plan: [DesktopShare/cso-operator-app-plan.md](https://github.com/cldr-steven-matison/DesktopShare/blob/main/cso-operator-app-plan.md)
- Streamers module — full spec, API endpoints, NiFi flow configs, session history: [DesktopShare/cso-operator-app-streamers.md](https://github.com/cldr-steven-matison/DesktopShare/blob/main/cso-operator-app-streamers.md)
- Blog — [RAG with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/RAG-with-Cloudera-Streaming-Operators/)
- Blog — [Insanely Fast Audio Transcription with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/Audio-Transcription-with-Cloudera-Streaming-Operators/)
- Backing YAMLs — [ClouderaStreamingOperators](https://github.com/cldr-steven-matison/ClouderaStreamingOperators)
- NiFi flow definitions — [NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates)

## Layout

```
backend/    FastAPI proxy + RAG orchestrator + Streamers pipeline (routers/streamers.py,
            services/streamers.py — gated behind MODULES=streamers, see above)
frontend/   Vite + React + TS + Tailwind + shadcn/ui
whisper/    Dockerfile + Service for the Whisper inference server
flows/      CSOOperatorApp.json — RAG/ingest flow export, three process groups
            (IngestDataToStream, StreamToWhisper, StreamTovLLM). Live PG is
            actually named CSOOperatorAppWindows in this NiFi instance, not
            CSOOperatorApp — file kept at its established name, just flagging
            the mismatch. Also TwitchChatBot.json — the Twitch chat-command
            stream-loader bot (see DesktopShare/streamers-twitch-bot.md);
            doesn't call this app's backend at all, lives here rather than
            streamers/ since it's not really a Streamers-module flow
streamers/  StreamersApp.json (NiFi flow export), WatchlistChatJoiner.json
            (separate PG, joins watchlisted streamers' Twitch chat — does
            call this app's /api/streamers/watchlist endpoints, hence living
            here despite being its own isolated PG, not nested under
            StreamersApp), config.yaml, kafka-topics.yaml, pvc.yaml
k8s/        Deployment, Service, ConfigMap; backing/ copies of stack YAMLs
samples/    Reference doc + audio for Demo Mode; efm-demos.json
            (catalog read at request time by /api/efm/demos)
scripts/    mac-dev.sh, deploy.sh, bootstrap-stack.sh, build-modules.py,
            setup-streamers-flows.py, kafka-external-listener.sh, diagnose-query.py
```

## Quick start (Mac dev)

```bash
make bootstrap     # apply backing YAMLs, patch Kafka external listener, build whisper image
# in three terminals:
make dev           # port-forwards (vllm/qdrant/embed/whisper + 4× kafka)
make backend       # FastAPI on :8000 with .env.local
make frontend      # Vite on :5173, proxies /api -> :8000
```

Backend env: copy `backend/.env.example` to `backend/.env.local` and fill in
`NIFI_PASSWORD` from the `nifi-admin-creds` Secret in `cfm-streaming`:

```bash
kubectl get secret nifi-admin-creds -n cfm-streaming \
  -o jsonpath='{.data.password}' | base64 -d
```

### Strict-CPU variant (Mac, no GPU)

For Mac dev without GPU passthrough, swap vLLM and Whisper for CPU-only
equivalents (llama.cpp + faster-whisper). Same backend, same NiFi flows,
same ConfigMap — only the in-cluster Deployments change. Pass `STACK=cpu`
to bootstrap and dev:

```bash
make bootstrap STACK=cpu     # no $HF_TOKEN needed
make dev STACK=cpu
make backend                 # unchanged
make frontend                # unchanged
```

Switch back with `make bootstrap STACK=gpu`. See
[DesktopShare/cso-operator-app-plan.md → CPU variant](https://github.com/cldr-steven-matison/DesktopShare/blob/main/cso-operator-app-plan.md#cpu-variant-mac-no-gpu)
for details and performance ceilings.

## Quick start (Windows)

Requires WSL2 or Git Bash so the bash scripts run. Same flow as Mac:

```bash
git clone https://github.com/cldr-steven-matison/cso-operator-app
cd cso-operator-app
export HF_TOKEN=...           # for the Whisper image build
make bootstrap
make dev
make backend                  # in another terminal
make frontend                 # in another terminal
```

Whisper requires a GPU-enabled Minikube. The other services
(vLLM, Qdrant, embedding-server, NiFi, Kafka) work the same on Windows
once the backing operators are installed.

## Deploy (Mac or Windows Minikube)

**Always pass `MODULES` explicitly — see [Modules](#modules) above for why a bare `make deploy` is a trap, not a convenience default.**

```bash
make deploy MODULES=rag,streamers,efm
```

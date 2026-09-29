# CSO Operator App

![CSO Operator App Control Plane](/CSO_Operator_Control_Plane.png)

A control panel for the **Cloudera Streaming Operators** stack on Minikube — a RAG + Audio Transcription demo with EFM agent control. See [Modules](#modules) below.

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

The Operator/RAG/EFM pieces are a local demo only — no auth, no production hardening. Live credentials (NiFi) are injected via `kubectl set env`, never in YAML/ConfigMaps.

## Modules

`MODULES` is a build-time + deploy-time flag controlling which optional tabs/routes are active. **Always state it explicitly on every `make deploy`/`make build`/`scripts/deploy.sh` call — there is no "correct" default, and a bare `make deploy` silently builds Operator-only, which has caused a real overnight outage before.**

| Value | Adds |
|---|---|
| *(empty)* | Operator tab only (pod/operator health) — this is what you get if you forget to pass `MODULES` |
| `rag` | RAG tab (document/audio ingest demo, NiFi controls, Kafka activity, Qdrant, RAG query) |
| `efm` | EFM tab (agent-class/agent list, Test Agent panel) |

Combine with commas, e.g. `MODULES=rag,efm` for everything. Operator is always present regardless of `MODULES`.

```bash
make deploy MODULES=rag,efm     # full install
make deploy MODULES=            # Operator only, explicit bare minimum
```

**Gotcha:** `MODULES` only gates the **frontend tabs** and the **health-check pings** — `backend/main.py` always registers the `query`/`nifi`/`qdrant`/`kafka`/`ingest`/`k8s`/`efm` routers no matter what `MODULES` says, so hiding a tab doesn't reduce the backend's surface area. `VITE_MODULES=all` is a frontend shorthand for "show every tab" (`frontend/src/App.tsx`).

## Sources

- Blog — [RAG with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/RAG-with-Cloudera-Streaming-Operators/)
- Blog — [Insanely Fast Audio Transcription with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/Audio-Transcription-with-Cloudera-Streaming-Operators/)
- Backing YAMLs — [ClouderaStreamingOperators](https://github.com/cldr-steven-matison/ClouderaStreamingOperators)
- NiFi flow definitions — [NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates)

## Layout

```
backend/    FastAPI proxy + RAG orchestrator (routers/, services/)
frontend/   Vite + React + TS + Tailwind + shadcn/ui
whisper/    Dockerfile + Service for the Whisper inference server
flows/      CSOOperatorApp.json — RAG/ingest flow export, three process groups
            (IngestDataToStream, StreamToWhisper, StreamTovLLM). Live PG is
            actually named CSOOperatorAppWindows in this NiFi instance, not
            CSOOperatorApp — file kept at its established name, just flagging
            the mismatch.
k8s/        Deployment, Service, ConfigMap; backing/ copies of stack YAMLs
samples/    Reference doc + audio for Demo Mode; efm-demos.json
            (catalog read at request time by /api/efm/demos)
scripts/    mac-dev.sh, deploy.sh, bootstrap-stack.sh, build-modules.py,
            kafka-external-listener.sh, diagnose-query.py
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

Switch back with `make bootstrap STACK=gpu`.

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
make deploy MODULES=rag,efm
```

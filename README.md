# CSO Operator App

![CSO Operator App Control Plane](/CSO_Operator_Control_Plane.png)

A demo control panel for the **Cloudera Streaming Operators** RAG + Audio Transcription stack on Minikube.

One screen drives:

- **Documents** ingested via NiFi `IngestToStream` → Kafka `new_documents` → chunked + embedded + upserted into Qdrant by `StreamTovLLM`.
- **Audio** ingested via NiFi `IngestDataToStream` → Kafka `new_audio` → transcribed by `StreamToWhisper` (insanely-fast-whisper, GPU) → republished to `new_documents` → indexed by the same RAG flow.
- **Streaming RAG queries** against vLLM (Qwen2.5-3B-Instruct), with sources from Qdrant.
- **NiFi flow controls** (start/stop, live state).
- **Kafka topic activity** (depth, lag, live tail).
- **Qdrant collection management** (recreate, stats).

Local demo only — no auth, no production hardening.

## Sources

- Plan: [DesktopShare/cso-operator-app-plan.md](https://github.com/cldr-steven-matison/DesktopShare/blob/main/cso-operator-app-plan.md)
- Blog — [RAG with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/RAG-with-Cloudera-Streaming-Operators/)
- Blog — [Insanely Fast Audio Transcription with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/Audio-Transcription-with-Cloudera-Streaming-Operators/)
- Backing YAMLs — [ClouderaStreamingOperators](https://github.com/cldr-steven-matison/ClouderaStreamingOperators)
- NiFi flow definitions — [NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates)

## Layout

```
backend/    FastAPI proxy + RAG orchestrator
frontend/   Vite + React + TS + Tailwind + shadcn/ui
whisper/    Dockerfile + Service for the Whisper inference server
flows/      CSOOperatorApp.json — single import containing all four
            process groups (IngestDocsToStream, IngestDataToStream,
            StreamToWhisper, StreamTovLLM)
k8s/        Deployment, Service, ConfigMap; backing/ copies of stack YAMLs
samples/    Reference doc + audio for Demo Mode
scripts/    mac-dev.sh, deploy.sh, bootstrap-stack.sh
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

```bash
make deploy
```

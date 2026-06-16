# CSO Operator App

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
make bootstrap     # apply backing YAMLs, build whisper image, import flows
make dev           # port-forwards + uvicorn + vite
```

## Deploy (Mac or Windows Minikube)

```bash
make deploy
```

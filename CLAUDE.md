# Read this first. Every session.

This is Steven's CSO Operator demo app — Operator controls, the EFM test kit, and the RAG/audio-transcription stack. Read `BrainShare/CLAUDE.md` first (project-wide rules — the repo is wherever BrainShare is checked out on this device, see `BrainShare/CLAUDE-CHECKIN.md`); this file is app-specific detail on top of it.

**Commit directly to `main` — don't auto-branch.** This repo works entirely on `main` (no feature branches); the harness's "branch first on the default branch" default is overridden here. Commit/push discipline otherwise follows `BrainShare/agent/workflow.md` (only when asked, or the issue-finish ritual; no `Co-Authored-By` trailers).

## Deploy

**Read the running pod's baked `MODULES` first, then deploy with exactly that value:**

```bash
POD=$(kubectl get pods -l app=cso-operator-app -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD" -- env | grep MODULES          # e.g. MODULES=rag,efm
MODULES=rag,efm bash scripts/deploy.sh             # the value you just read, verbatim
```

`MODULES` is a Docker build-arg baked into the image (the frontend's `VITE_MODULES` decides which tabs render); it is not in the Deployment's `env:` and `k8s/configmap.yaml`'s runtime `MODULES` is a separate gate. It only gates the frontend tabs and which health checks run — the backend registers all of its routers regardless. `deploy.sh` falls back to `rag,efm` when the variable is unset — that fallback is a stopgap, not a default to converge on: an unset or narrowed value silently drops the RAG/EFM tabs while `kubectl rollout status` reports healthy (overnight outage 2026-07-17, again 07-18 and 07-26). Never narrow it because the task only touched one module; add or remove a module only when Steven says so. After deploy, confirm exactly one pod `Running` (not `Terminating`) and grep the served bundle for the tab strings, not just the API. Every redeploy also needs the live NiFi flow-state check and a fresh ask (`BrainShare/agent/incident-rules.md` "Live service restarts").

**`VLLM_MODEL` (`k8s/configmap.yaml`) must match the model vLLM actually serves.** vLLM 404s any other name and `_generate_title` swallows the error, so a completion silently comes back empty with nothing in the UI to say why (2026-08-26→27). Prod vLLM is `Qwen/Qwen2.5-3B-Instruct-AWQ` (manifest `~/ClouderaStreamingOperators/vllm-Qwen2.5-3B-Instruct-AWQ.yaml` on WindowsDesktop); `deploy.sh` applies this configmap on every deploy, so a stale value here silently overrides a live `kubectl edit`.

## Credentials

NiFi credentials are injected live via `kubectl set env deploy/cso-operator-app KEY=value` — never in `deployment.yaml`/`configmap.yaml`. A `kubectl apply` reporting `deployment.apps/cso-operator-app unchanged` means these survived untouched; that's the thing to check after any redeploy, not just rollout status.

## NiFi flow definitions go stale — re-export them, don't just leave them

`flows/CSOOperatorApp.json` is the export of this app's live RAG/ingest process group. It drifts fast — the flow gets hand-edited live in the NiFi UI/API (new processors, rewired connections, new sub-PGs), never by editing the JSON file directly.

Re-export periodically, and definitely after any session that builds/rewires it:

1. Find the target PG's real runtime ID — dump the live flow (`kubectl exec mynifi-0 -n cfm-streaming -- gunzip -c /opt/nifi/nifi-current/data/flow.json.gz`) and read its `instanceIdentifier`, or walk `GET /nifi-api/flow/process-groups/root`.
2. `GET /nifi-api/process-groups/{id}/download` — returns the same VersionedFlowSnapshot JSON the NiFi UI's "Download flow definition" produces.
3. **Pretty-print before committing** (`json.dumps(d, indent=2)`) — the raw response is minified, and committing it that way makes every future diff unreviewable (whole-file rewrite instead of the real additive change).
4. Confirmed safe to commit: Parameter Context sensitive values export as `null`, never real secret values, and processor properties aren't masked-then-leaked either. No credential risk in this file.

## Live traffic caution

Every redeploy restarts a live service. The full live-service rules — post-redeploy pod sanity, the live NiFi flow-state check, a fresh ask each time — live in `BrainShare/agent/incident-rules.md` ("Live service restarts"). Read that before touching anything that redeploys.

Issues for this app are filed in `cldr-steven-matison/BrainShare`; this repo's tracker is empty.

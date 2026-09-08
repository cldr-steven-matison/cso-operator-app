# Read this first. Every session.

This is Steven's live production app — real X posts go out from here. Read `DesktopShare/CLAUDE.md` first (project-wide rules — the repo is wherever DesktopShare is checked out on this device, see `DesktopShare/CLAUDE-CHECKIN.md`); this file is app-specific detail on top of it.

## Before touching backend/services/streamers.py

This file (2000+ lines) is where every real incident in this project has originated (credential wipes, duration/OOM bugs, caption regressions). Before adding anything to it:

1. **Grep the file for existing conventions first.** It already has established, working patterns for: ffmpeg thread-capping (`-threads 1 -x264opts threads=1:sliced-threads=0` — see `_burn_platform_overlay`, `_burn_glitch_intro`'s `encode_still`), file locking (`_pending_lock`/`_overlay_lock` via `flock`), atomic-ish JSON persistence patterns, and NiFi run-status-only edits (never GET-then-PUT a full processor entity). Don't re-derive a weaker version of something already solved here.
2. **The pod's real resource limits are `cpu: "1"`, `memory: "1Gi"`** (`k8s/deployment.yaml`) — far below what libx264's auto-detected thread count (from host CPU count, not the cgroup limit) assumes. Any new ffmpeg/subprocess call doing real encoding work needs an explicit thread cap or it risks a silent OOM-kill (`returncode -9`) that looks like a mysterious transcoding failure.
3. **JSON state files** (`.pending_publish.json`, `.published.json`, `.watchlist.json`, etc., all in `/clips` on the PVC) go through `_atomic_write_json()` (`backend/services/streamers.py:103`) — temp-then-rename, so a crash mid-write can't truncate one to invalid JSON that the loaders would read as empty state. Any new state file must use it too; a plain `write_text()` reintroduces the gap. Background: `streamers/cso-operator-app-streamers-review-2026-07-17.md` in DesktopShare.

## Deploy

**Read the running pod's baked `MODULES` first, then deploy with exactly that value:**

```bash
POD=$(kubectl get pods -l app=cso-operator-app -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD" -- env | grep MODULES          # e.g. MODULES=rag,streamers,efm
MODULES=rag,streamers,efm bash scripts/deploy.sh    # the value you just read, verbatim
```

`MODULES` is a Docker build-arg baked into the image (the frontend's `VITE_MODULES` decides which tabs render); it is not in the Deployment's `env:` and `k8s/configmap.yaml`'s runtime `MODULES` is a separate gate. `deploy.sh` falls back to `streamers` when the variable is unset — that fallback is a stopgap, not a default to converge on: an unset or narrowed value silently drops RAG/EFM tabs while `kubectl rollout status` reports healthy (overnight outage 2026-07-17, again 07-18 and 07-26). Never narrow it because the task only touched one module; add or remove a module only when Steven says so. After deploy, confirm exactly one pod `Running` (not `Terminating`) and grep the served bundle for the tab strings, not just the API. Every redeploy also needs the live NiFi flow-state check and a fresh ask (`DesktopShare/agent/incident-rules.md` "Live service restarts").

**`VLLM_MODEL` (`k8s/configmap.yaml`) must match the model vLLM actually serves.** vLLM 404s any other name and `_generate_title`/caption swallow the error, so every clip becomes "quoted / topic unclear" with nothing in the UI to say why (2026-08-26→27, ~30 clips). Prod vLLM is `Qwen/Qwen2.5-3B-Instruct`; verify a caption path change with a direct `POST /api/streamers/process-clip` on an existing queue record and look for `caption_mode: reaction`.

## Credentials

X/Twitch/Kick/NiFi credentials are injected live via `kubectl set env deploy/cso-operator-app KEY=value` — never in `deployment.yaml`/`configmap.yaml`. A `kubectl apply` reporting `deployment.apps/cso-operator-app unchanged` means these survived untouched; that's the thing to check after any redeploy, not just rollout status.

## NiFi flow definitions go stale — re-export them, don't just leave them

`flows/CSOOperatorApp.json`, `flows/TwitchChatBot.json`, `streamers/StreamersApp.json`, `streamers/WatchlistChatJoiner.json` are exports of this app's four live NiFi process groups. They drift fast — these flows get hand-edited live in the NiFi UI/API (new processors, rewired connections, new sub-PGs), never by editing these JSON files directly. As of 2026-07-24 they'd gone weeks stale, missing entire PGs (`LiveStreamerAlert`, `TunaStarLinkFlows`, the `Trigger`/`RouteOnAttribute` on-demand entry point) before being refreshed.

Re-export periodically, and definitely after any session that builds/rewires a flow with a checked-in export here:

1. Find the target PG's real runtime ID — dump the live flow (`kubectl exec mynifi-0 -n cfm-streaming -- gunzip -c /opt/nifi/nifi-current/data/flow.json.gz`) and read its `instanceIdentifier`, or walk `GET /nifi-api/flow/process-groups/root`.
2. `GET /nifi-api/process-groups/{id}/download` — returns the same VersionedFlowSnapshot JSON the NiFi UI's "Download flow definition" produces.
3. **Pretty-print before committing** (`json.dumps(d, indent=2)`) — the raw response is minified, and committing it that way makes every future diff unreviewable (whole-file rewrite instead of the real additive change).
4. Confirmed safe to commit: Parameter Context sensitive values export as `null`, never real secret values, and processor properties aren't masked-then-leaked either. No credential risk in these files.

Worked example with the exact commands: `DesktopShare/streamers/cso-operator-app-streamers.md` Session 21 (2026-07-24).

## Live traffic caution

Fetch/publish can be running at any time. The full live-queue rules — no `kubectl exec` patches on `/clips`, no unilateral queue mutations, no injecting test data into live triggers, post-redeploy pod sanity — live in `DesktopShare/agent/live-queues.md`. Read that before touching anything queue-adjacent here.

## Kafka topics (cld-streaming)

- **Wipe a topic:** `AIOKafkaAdminClient(bootstrap_servers=…).delete_topics([...])` from the app (what the Reset Kafka button does — same as Surveyor, broker-direct, gone immediately, auto-recreated on next write), or `kubectl delete kafkatopic new-clips processed-clips -n cld-streaming --wait=true` (`--wait=true` is required or Strimzi hasn't finished). What does **not** work: a `retention.ms=1000` flush (async cleaner), `kafka-topics.sh --delete` alone (the Strimzi CRD recreates it), `kubectl delete kafkatopic` without `--wait`, deleting the CR object from Python (broker data persists), re-applying CRDs during the deletion window. Clear `/clips/*.mp4` + `.seen_clips.json` with it.
- **Names:** Strimzi CR names are hyphenated (`new-clips`), Kafka topic names are underscored (`new_clips`, via `spec.topicName`). Topics have **1 partition** regardless of `spec.partitions`.
- **Consumer pattern for `clip_queue`:** manual `TopicPartition` assignment + `getmany(tp, timeout_ms=…, max_records=N)`. `subscribe()` needs a group coordinator and `stop()` raises `CancelledError`; `async for` hangs after a manual `seek()`.
- **CPU-heavy per-item work on a NiFi cron (ffmpeg burns) runs as a trickle:** smallest batch, most frequent run — the pod's `cpu: "1"` limit made a 4-streamer/15-min FetchClips run take 27–31 min and sit at the 1 Gi ceiling; 1 streamer/4 min fixed it (2026-07-28). Deploy the code change **before** changing the live NiFi schedule.

Issues for this app are filed in `cldr-steven-matison/DesktopShare` (`[Streamers]` title prefix); this repo's tracker is empty. Full history and incident writeups: `DesktopShare/streamers/cso-operator-app-streamers.md` (golden source doc).

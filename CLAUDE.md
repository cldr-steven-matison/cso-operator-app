# Read this first. Every session.

This is Steven's live production app — real X posts go out from here. Read `DesktopShare/CLAUDE.md` first (project-wide rules — the repo is wherever DesktopShare is checked out on this device, see `DesktopShare/CLAUDE-CHECKIN.md`); this file is app-specific detail on top of it.

## Before touching backend/services/streamers.py

This file (2000+ lines) is where every real incident in this project has originated (credential wipes, duration/OOM bugs, caption regressions). Before adding anything to it:

1. **Grep the file for existing conventions first.** It already has established, working patterns for: ffmpeg thread-capping (`-threads 1 -x264opts threads=1:sliced-threads=0` — see `_burn_platform_overlay`, `_burn_glitch_intro`'s `encode_still`), file locking (`_pending_lock`/`_overlay_lock` via `flock`), atomic-ish JSON persistence patterns, and NiFi run-status-only edits (never GET-then-PUT a full processor entity). Don't re-derive a weaker version of something already solved here.
2. **The pod's real resource limits are `cpu: "1"`, `memory: "1Gi"`** (`k8s/deployment.yaml`) — far below what libx264's auto-detected thread count (from host CPU count, not the cgroup limit) assumes. Any new ffmpeg/subprocess call doing real encoding work needs an explicit thread cap or it risks a silent OOM-kill (`returncode -9`) that looks like a mysterious transcoding failure.
3. **JSON state files** (`.pending_publish.json`, `.published.json`, `.watchlist.json`, etc., all in `/clips` on the PVC) are written with plain `write_text()`, not atomic temp-then-rename. A crash mid-write silently truncates to invalid JSON, which the loaders treat as empty state, not an error. Known gap, not yet fixed — see `cso-operator-app-streamers-review-2026-07-17.md` in DesktopShare.

## Deploy

`MODULES=streamers bash scripts/deploy.sh` — **always state the MODULES value explicitly**, the default silently building with zero optional modules caused a real overnight outage (2026-07-17). See `project_cso_operator_modules_flag.md`. After deploy, confirm exactly one pod `Running` (not `Terminating`) before triggering another deploy.

## Credentials

X/Twitch/Kick/NiFi credentials are injected live via `kubectl set env deploy/cso-operator-app KEY=value` — never in `deployment.yaml`/`configmap.yaml`. A `kubectl apply` reporting `deployment.apps/cso-operator-app unchanged` means these survived untouched; that's the thing to check after any redeploy, not just rollout status.

## NiFi flow definitions go stale — re-export them, don't just leave them

`flows/CSOOperatorApp.json`, `flows/TwitchChatBot.json`, `streamers/StreamersApp.json`, `streamers/WatchlistChatJoiner.json` are exports of this app's four live NiFi process groups. They drift fast — these flows get hand-edited live in the NiFi UI/API (new processors, rewired connections, new sub-PGs), never by editing these JSON files directly. As of 2026-07-24 they'd gone weeks stale, missing entire PGs (`LiveStreamerAlert`, `TunaStarLinkFlows`, the `Trigger`/`RouteOnAttribute` on-demand entry point) before being refreshed.

Re-export periodically, and definitely after any session that builds/rewires a flow with a checked-in export here:

1. Find the target PG's real runtime ID — dump the live flow (`kubectl exec mynifi-0 -n cfm-streaming -- gunzip -c /opt/nifi/nifi-current/data/flow.json.gz`) and read its `instanceIdentifier`, or walk `GET /nifi-api/flow/process-groups/root`.
2. `GET /nifi-api/process-groups/{id}/download` — returns the same VersionedFlowSnapshot JSON the NiFi UI's "Download flow definition" produces.
3. **Pretty-print before committing** (`json.dumps(d, indent=2)`) — the raw response is minified, and committing it that way makes every future diff unreviewable (whole-file rewrite instead of the real additive change).
4. Confirmed safe to commit: Parameter Context sensitive values export as `null`, never real secret values, and processor properties aren't masked-then-leaked either. No credential risk in these files.

Worked example with the exact commands: `DesktopShare/cso-operator-app-streamers.md` Session 21 (2026-07-24).

## Live traffic caution

Fetch/publish can be running at any time. The full live-queue rules — no `kubectl exec` patches on `/clips`, no unilateral queue mutations, no injecting test data into live triggers, post-redeploy pod sanity — live in `DesktopShare/agent/live-queues.md`. Read that before touching anything queue-adjacent here.

Full history and incident writeups: `DesktopShare/cso-operator-app-streamers.md` (golden source doc) and this session's Claude memory index on the local device.

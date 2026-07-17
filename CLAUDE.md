# Read this first. Every session.

This is Steven's live production app — real X posts go out from here. Read `/home/tunas/DesktopShare/CLAUDE.md` first (project-wide rules); this file is app-specific detail on top of it.

## Before touching backend/services/streamers.py

This file (2000+ lines) is where every real incident in this project has originated (credential wipes, duration/OOM bugs, caption regressions). Before adding anything to it:

1. **Grep the file for existing conventions first.** It already has established, working patterns for: ffmpeg thread-capping (`-threads 1 -x264opts threads=1:sliced-threads=0` — see `_burn_platform_overlay`, `_burn_glitch_intro`'s `encode_still`), file locking (`_pending_lock`/`_overlay_lock` via `flock`), atomic-ish JSON persistence patterns, and NiFi run-status-only edits (never GET-then-PUT a full processor entity). Don't re-derive a weaker version of something already solved here.
2. **The pod's real resource limits are `cpu: "1"`, `memory: "1Gi"`** (`k8s/deployment.yaml`) — far below what libx264's auto-detected thread count (from host CPU count, not the cgroup limit) assumes. Any new ffmpeg/subprocess call doing real encoding work needs an explicit thread cap or it risks a silent OOM-kill (`returncode -9`) that looks like a mysterious transcoding failure.
3. **JSON state files** (`.pending_publish.json`, `.published.json`, `.watchlist.json`, etc., all in `/clips` on the PVC) are written with plain `write_text()`, not atomic temp-then-rename. A crash mid-write silently truncates to invalid JSON, which the loaders treat as empty state, not an error. Known gap, not yet fixed — see `cso-operator-app-streamers-review-2026-07-17.md` in DesktopShare.

## Deploy

`MODULES=streamers bash scripts/deploy.sh` — **always state the MODULES value explicitly**, the default silently building with zero optional modules caused a real overnight outage (2026-07-17). See `project_cso_operator_modules_flag.md`. After deploy, confirm exactly one pod `Running` (not `Terminating`) before triggering another deploy.

## Credentials

X/Twitch/Kick/NiFi credentials are injected live via `kubectl set env deploy/cso-operator-app KEY=value` — never in `deployment.yaml`/`configmap.yaml`. A `kubectl apply` reporting `deployment.apps/cso-operator-app unchanged` means these survived untouched; that's the thing to check after any redeploy, not just rollout status.

## Live traffic caution

Fetch/publish can be running at any time. No manual `kubectl exec` patches to `/clips` while that's possible — ship fixes through the normal rebuild+redeploy pipeline. No cancelling/mutating already-queued pending-publish items without an explicit per-instance ask from Steven, even when a fix you just shipped clearly flags them as bad.

Full history and incident writeups: `DesktopShare/cso-operator-app-streamers.md` (golden source doc) and the memory files under `/home/tunas/.claude/projects/-home-tunas-DesktopShare/memory/` (all `project_streamers_*` and `feedback_*` files).

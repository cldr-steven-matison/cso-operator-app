import asyncio
import json
import os
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from config import settings
from services import chat_activity, inspector, streamers

router = APIRouter(prefix="/streamers")


# ── NiFi flows ───────────────────────────────────────────────────────────────

@router.get("/flows")
async def flows(request: Request):
    """NiFi status for FetchClips, ProcessClips, PublishClipOffPeakDay, PublishClipPeakTimeCron."""
    return await streamers.flows_state(request.app.state.http)


@router.post("/flows/{name}/start")
async def flow_start(name: str, request: Request):
    try:
        return await streamers.flow_set_state(request.app.state.http, name, running=True)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/flows/{name}/stop")
async def flow_stop(name: str, request: Request):
    try:
        if name == "FetchClips":
            # Asymmetric with start: only pause the GenerateFlowFile timer so an
            # in-flight fetch isn't cut off mid-run, not the whole PG.
            return await streamers.stop_fetch_clips_generator(request.app.state.http)
        return await streamers.flow_set_state(request.app.state.http, name, running=False)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/flows/LiveStreamerAlert/run-once")
async def live_streamer_alert_run_once(request: Request):
    """Manual Telegram-triggered pulse of PollTimer for one poll cycle."""
    try:
        return await streamers.run_live_streamer_alert_once(request.app.state.http)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/flows/trigger/{name}")
async def flow_trigger(name: str, request: Request):
    """One-shot on-demand run via StreamersApp's shared Trigger (ListenHTTP)
    entry point -- name must be one of streamers.TRIGGER_REQUESTS."""
    try:
        return await streamers.trigger_flow(request.app.state.http, name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/flows/LiveStreamerAlert/refresh-oauth")
async def live_streamer_alert_refresh_oauth(request: Request):
    """Manual off-cycle run of the same disable/re-enable forced-token-refresh
    start_oauth_refresh_scheduler() does daily -- for verifying the fix works
    without waiting for the next scheduled cycle, or for an on-call to force it
    if a token goes bad between scheduled runs."""
    try:
        return await streamers.refresh_live_alert_oauth_tokens(request.app.state.http)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Clip queue ────────────────────────────────────────────────────────────────

@router.get("/queue")
async def clip_queue():
    """Peek processed_clips topic and return parsed clip records."""
    return await streamers.clip_queue()


# ── Publish ───────────────────────────────────────────────────────────────────

class PublishRequest(BaseModel):
    clip_path: str
    tweet_text: str
    clip_id: str = ""
    title: str = ""
    source: str = ""
    streamer: str = ""
    url: str = ""
    thumbnail_url: str = ""
    x_handle: str = ""
    view_count: int = 0
    duration: float = 0
    created_at: str = ""


@router.post("/approve")
async def approve(body: PublishRequest):
    """Queue a clip for publishing. Returns immediately; NiFi drains the queue every 2 min."""
    if not body.clip_path or not body.tweet_text:
        raise HTTPException(status_code=400, detail="clip_path and tweet_text are required")
    if not os.path.exists(body.clip_path):
        raise HTTPException(status_code=404, detail=f"Clip file not found: {body.clip_path} — re-fetch clips first")
    return streamers.approve_clip(
        body.clip_id, body.clip_path, body.tweet_text, body.title,
        body.source, body.streamer, body.url, body.thumbnail_url, body.x_handle,
        body.view_count, body.duration, body.created_at,
    )


@router.post("/publish-next")
async def publish_next():
    """Pop and publish the next queued clip. Called by NiFi GenerateFlowFile timer every 2 min."""
    try:
        return await streamers.publish_next()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/pending")
async def pending_queue():
    """List clips queued for X publish, in post order."""
    return {"pending": streamers.get_pending()}


@router.post("/pending/{clip_id}/cancel")
async def cancel_pending(clip_id: str):
    """Remove a clip from the publish queue before NiFi drains it."""
    return streamers.cancel_pending(clip_id)


@router.post("/pending/{clip_id}/publish-now")
async def pending_publish_now(clip_id: str):
    """Publish one specific pending clip immediately, regardless of its queue position."""
    try:
        return await streamers.publish_pending(clip_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/publish")
async def publish(body: PublishRequest):
    """Direct publish (bypasses queue). Kept for manual/debug use."""
    if not body.clip_path or not body.tweet_text:
        raise HTTPException(status_code=400, detail="clip_path and tweet_text are required")
    if not os.path.exists(body.clip_path):
        raise HTTPException(status_code=404, detail=f"Clip file not found: {body.clip_path} — re-fetch clips first")
    # Gif-only streamers (clip=N in streamer_paths / streamers.md) never post
    # the MP4 — the review card's Post Now button hits this endpoint with the
    # clip path, so redirect it to the reaction GIF the same way approve does.
    clip_path = body.clip_path
    paths = streamers.streamer_paths(body.streamer)
    if not paths["clip"]:
        gif_path = Path(clip_path).with_suffix(".gif")
        if paths["gif"] and gif_path.exists():
            clip_path = str(gif_path)
        else:
            raise HTTPException(
                status_code=409,
                detail=f"clip posting is disabled for {body.streamer} and no .gif exists for this clip",
            )
    try:
        return await streamers.publish_clip(
            clip_path, body.tweet_text, body.clip_id, body.title,
            body.source, body.streamer, body.url, body.thumbnail_url, body.x_handle,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/published")
async def published_clips():
    """Most-recently-published clips, for the Posted Clips tile gallery."""
    return {"published": streamers.get_published_history()}


@router.post("/admin/backfill-metadata")
async def backfill_metadata():
    """One-time repair for pending/published entries that predate source/streamer/
    url/thumbnail_url/x_handle being added to approve_clip()/mark_published().
    Safe to re-run — a no-op once every entry already has its fields."""
    try:
        return await streamers.backfill_metadata()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Skip ──────────────────────────────────────────────────────────────────────

class SkipRequest(BaseModel):
    clip_id: str


@router.post("/skip")
async def skip_clip(body: SkipRequest):
    """Mark a clip as skipped so it no longer appears in the review queue."""
    if not body.clip_id:
        raise HTTPException(status_code=400, detail="clip_id required")
    streamers.mark_skipped(body.clip_id)
    return {"ok": True, "clip_id": body.clip_id}


# ── Clip video file serve ─────────────────────────────────────────────────────

@router.get("/clip/{clip_id}")
async def serve_clip(clip_id: str):
    """Stream the MP4 file for a clip. Used by the frontend video player."""
    if not re.match(r'^[A-Za-z0-9_\-]+$', clip_id):
        raise HTTPException(status_code=400, detail="Invalid clip_id")
    path = Path(settings.CLIP_STORAGE_PATH) / f"{clip_id}.mp4"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Clip not found on disk")
    return FileResponse(path, media_type="video/mp4")


# ── NiFi-callable pipeline endpoints ─────────────────────────────────────────
# These are called by the FetchClips and ProcessClips NiFi flows.

@router.post("/fetch-clips")
async def fetch_clips():
    """Poll Twitch for new clips, download to PVC, publish metadata to new_clips.
    Called by the FetchClips NiFi GenerateFlowFile → InvokeHTTP flow every 15 min."""
    result = await streamers.fetch_clips()
    return result


@router.post("/process-clip")
async def process_clip(request: Request):
    """Receive clip metadata JSON from NiFi (new_clips topic), run Whisper + vLLM,
    return enriched JSON. Called by the ProcessClips NiFi ConsumeKafka → InvokeHTTP flow."""
    body = await request.body()
    try:
        clip = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Expected JSON clip metadata")
    result = await streamers.process_clip(clip)
    return result


@router.post("/process-gif")
async def process_gif(request: Request):
    """Cut the reaction GIF for one clip and index it. Called by the SECOND
    InvokeHTTP branch in the ProcessClips NiFi flow — same new_clips FlowFile
    as /process-clip, cloned. Independent of it by design: a gif that can't be
    cut never delays or disqualifies the clip, and vice versa (#195)."""
    body = await request.body()
    try:
        clip = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Expected JSON clip metadata")
    return await streamers.process_gif(clip)


# ── GIF library (the GIFs tab) ───────────────────────────────────────────────

class GifReviewRequest(BaseModel):
    verdict: str = ""


@router.get("/gifs")
async def list_gifs(include_hidden: bool = False):
    """The reaction-GIF library for the GIFs panel. Newest first; hidden and
    already-posted GIFs drop out unless include_hidden=1."""
    return {"gifs": streamers.list_gifs(include_hidden)}


@router.post("/gifs/{clip_id}/review")
async def review_gif(clip_id: str, body: GifReviewRequest):
    """✅ good / ❌ hidden from the GIFs panel. Hidden also blocks the GIF from
    ever being auto-queued to X on approve."""
    try:
        return streamers.set_gif_verdict(clip_id, body.verdict)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/gifs/{clip_id}/post-now")
async def post_gif_now(clip_id: str):
    """Post one GIF to X immediately, tagging the streamer and referencing the
    clip context. Called by the GIFs panel's Post Now button."""
    entry = streamers.get_gif_entry(clip_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"No GIF indexed for {clip_id}")
    if not os.path.exists(entry.get("gif_path", "")):
        raise HTTPException(status_code=404, detail="GIF file is gone from the PVC")
    try:
        return await streamers.publish_gif_now(clip_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/face-layouts")
async def face_layouts():
    """What we've learned about each streamer's scene — the median facecam box
    (as frame fractions) and how many clips it's based on. Read-only view of
    .face_layout.json, for tuning crops per streamer."""
    return {"layouts": streamers.get_face_layouts()}


@router.get("/gif/{clip_id}")
async def serve_gif(clip_id: str):
    """Stream the .gif for a clip. Mirrors /clip/{clip_id}, which only ever
    serves .mp4 — without this the GIFs panel has nothing to render."""
    if not re.match(r'^[A-Za-z0-9_\-]+$', clip_id):
        raise HTTPException(status_code=400, detail="Invalid clip_id")
    path = Path(settings.CLIP_STORAGE_PATH) / f"{clip_id}.gif"
    if not path.exists():
        raise HTTPException(status_code=404, detail="GIF not found on disk")
    # no-cache, not no-store: the browser may keep the bytes but MUST revalidate
    # against the ETag. A re-cut rewrites this exact path, and without this the
    # heuristic freshness a bare Last-Modified earns keeps serving the old gif
    # (the 2026-08-21 square recut looked like it hadn't shipped).
    return FileResponse(path, media_type="image/gif",
                        headers={"Cache-Control": "no-cache, must-revalidate"})


# ── Topic stats ──────────────────────────────────────────────────────────────

@router.get("/topics")
async def topic_stats():
    """Message counts and sample records for new_clips and processed_clips."""
    return await streamers.topic_stats()


# ── Kafka reset ───────────────────────────────────────────────────────────────

@router.post("/reset")
async def reset_kafka():
    """Delete Strimzi KafkaTopic CRDs and wipe /clips. Topics auto-recreate on next fetch."""
    return await streamers.reset_kafka()


# ── Watch list ────────────────────────────────────────────────────────────────

@router.get("/watchlist")
async def get_watchlist():
    return {"logins": streamers.get_watchlist()}


@router.get("/roster")
async def get_roster():
    """Every catalog streamer, not just the watch list -- for LiveStreamerAlert's
    live-status poll, same {"logins": [...]} shape as /watchlist."""
    return {"logins": streamers.get_roster()}


@router.get("/discover/top")
async def discover_top(request: Request, limit: int = 5):
    """Top live Twitch streams not already in the known roster -- for
    TopStreamerJoiner's presence-expansion bot. Read-only, never touches the
    watchlist/roster itself."""
    limit = max(1, min(limit, 20))
    logins = await streamers.discover_top_unfollowed(request.app.state.http, limit)
    return {"logins": logins}


class WatchlistUpdate(BaseModel):
    logins: list[str]


@router.post("/watchlist")
async def set_watchlist(body: WatchlistUpdate):
    streamers.set_watchlist(body.logins)
    return {"logins": streamers.get_watchlist()}


@router.post("/watchlist/rotate")
async def rotate_watchlist():
    """Swap the watch list for 4 new streamers. Takes effect on the next FetchClips stop/start."""
    return {"logins": streamers.rotate_watchlist()}


class WatchlistAdd(BaseModel):
    login: str
    platform: str  # "twitch" or "kick" — matches the flowfile attributes LiveStreamerAlert already has


@router.post("/watchlist/add")
async def add_to_watchlist(body: WatchlistAdd):
    """Pin one streamer onto the watch list without disturbing the rest — for LiveStreamerAlert
    to call when it finds someone live, passive/additive unlike POST /watchlist (full replace)."""
    entry = f"kick:{body.login}" if body.platform == "kick" else body.login
    return {"logins": streamers.add_to_watchlist(entry)}


@router.post("/watchlist/remove")
async def remove_from_watchlist(body: WatchlistAdd):
    """Unpin one streamer from the watch list without disturbing the rest — the
    offline-side counterpart to /watchlist/add, for per-streamer flows that pin
    on live and unpin on offline (e.g. tunastarlink's dedicated live-check)."""
    entry = f"kick:{body.login}" if body.platform == "kick" else body.login
    return {"logins": streamers.remove_from_watchlist(entry)}


@router.get("/x-handle/{login}")
async def get_x_handle(login: str):
    """Passive catalog lookup for LiveStreamerAlert (NiFi) — X handle has no @, empty string if unknown."""
    return {"login": login, "x_handle": streamers.get_x_handle(login)}


@router.get("/live")
async def get_live_status(login: str, request: Request):
    """Whether 'login' (bare Twitch login or 'kick:slug') is live right now —
    for TwitchChatListenerProcessor's !load live-check. Side effect: if found
    offline, also unpins it from the watch list (best-effort, no-op if it
    wasn't on there) — !load discovering someone's offline is exactly the
    signal the existing Twitch-only WatchlistChatJoiner prune never gets for
    Kick entries, since it can't join Kick chat to notice on its own."""
    live = await streamers.is_streamer_live(request.app.state.http, login)
    removed = False
    if not live:
        before = streamers.get_watchlist()
        streamers.remove_from_watchlist(login)
        removed = login not in streamers.get_watchlist() and login in before
    return {"login": login, "live": live, "removed_from_watchlist": removed}


@router.get("/live-bulk")
async def get_live_status_bulk(logins: str, request: Request):
    """Live status for several 'login'/'kick:slug' entries at once, comma-separated —
    pure read, no watchlist side effect (unlike /live). For the watchlist UI's
    online/offline pills, which shouldn't unpin anyone just from being viewed."""
    entries = [e for e in logins.split(",") if e.strip()]
    results = await asyncio.gather(
        *(streamers.is_streamer_live(request.app.state.http, e) for e in entries)
    )
    return {"statuses": dict(zip(entries, results))}


@router.get("/live-now")
async def live_now(request: Request):
    """Every roster entry (Twitch + Kick) that's live right now — for the
    Inspector's 'Live Now' picker."""
    return {"live": await inspector.list_live_now(request.app.state.http)}


@router.get("/inspect")
async def inspect_streamer(login: str, request: Request, clip_limit: int = 12):
    """Live status + recent clips (metadata only) + alt-platform check — for
    the Inspector sub-page. Historical/fast, no chat capture (see
    /inspect/chat on the Users/Bots page for that). Read-only, never touches
    the watchlist or fetch pipeline."""
    clip_limit = max(1, min(clip_limit, 30))
    return await inspector.inspect_streamer(request.app.state.http, login, clip_limit)


@router.get("/inspect/chat")
async def inspect_chat(login: str, request: Request, chat_seconds: int = 25):
    """Who's actually in a streamer's chat right now, bots flagged separately —
    for the Users/Bots page. Read-only, offline streamers skip capture."""
    chat_seconds = max(10, min(chat_seconds, 60))
    return await inspector.inspect_chat(request.app.state.http, login, chat_seconds)


@router.get("/chat-activity/{login}")
async def get_chat_activity(login: str):
    """Latest WatchlistChatSnapshotPoller cycle + short recent history for a
    watchlisted streamer — for the Users/Bots page's live mode. Populated by
    the NiFi poller re-running /inspect/chat on a timer, not by this endpoint
    itself. 404 if nothing's been recorded yet (not watchlisted, or the
    poller hasn't run a cycle for it)."""
    snapshot = chat_activity.get_snapshot(login)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="no chat activity recorded for this login yet")
    return snapshot


@router.get("/chat-activity/{login}/tail")
async def tail_chat_activity(login: str):
    """SSE live tail of new WatchlistChatSnapshotPoller cycles for one
    streamer — same StreamingResponse shape as /api/kafka/tail/{topic}."""
    async def stream():
        async for snapshot in chat_activity.tail_streamer(login):
            yield f"data: {json.dumps(snapshot)}\n\n".encode()

    return StreamingResponse(stream(), media_type="text/event-stream")


class QueueClipRequest(BaseModel):
    platform: str  # "twitch" or "kick"
    streamer: str
    clip_id: str
    url: str
    thumbnail_url: str = ""
    title: str = ""
    view_count: int = 0
    created_at: str = ""


@router.post("/inspect/queue-clip")
async def queue_specific_clip(body: QueueClipRequest, request: Request):
    """Download + process one manually-picked clip from the Inspector straight
    into the normal new_clips pipeline, bypassing the automated 45-90s duration
    filter — for clips (e.g. a viral one) the batch fetch would never pick up."""
    return await inspector.queue_specific_clip(
        request.app.state.http, body.platform, body.streamer, body.clip_id,
        body.url, body.thumbnail_url, body.title, body.view_count, body.created_at,
    )


# ── In-chat bot triggers (#174) ──────────────────────────────────────────────
# Called by the NiFi TwitchChatListenerProcessor when a viewer types a command
# in a watched channel — never by the UI. Two rules shape both routes:
#
# 1. A refusal is HTTP 200 with ok:false, not a 4xx. InvokeHTTP treats any
#    non-2xx as a retryable failure and would sit there re-firing a request that
#    can never succeed (a typo'd login, an offline streamer, a full watch list).
#    A 4xx/5xx is reserved for genuinely transient/broken plumbing.
# 2. `message` is the literal text the bot says in chat, and every one of them
#    names the streamer. Twitch silently drops a bot's byte-identical repeat of
#    its own message inside ~30s, so a fixed string ("they're not live") would
#    go missing for the second viewer who asks — the login is the varying token
#    that keeps each reply deliverable.

# The watch list is round-robined one entry per FetchClips run
# (_FETCH_BATCH_SIZE = 1), so its length IS every streamer's fetch cadence:
# going from the usual 4 entries to 12 already makes each streamer's clips
# arrive 3x less often. Uncapped, a busy chat could push the list to 50 and
# starve the very pipeline this feature exists to feed.
_CHAT_WATCHLIST_MAX = 12


class ChatTriggerRequest(BaseModel):
    login: str
    platform: str = ""       # "twitch" (default) or "kick"
    requested_by: str = ""   # chatter who typed the command
    channel: str = ""        # channel the command was typed in


@router.post("/chat-trigger/watchlist")
async def chat_trigger_watchlist(body: ChatTriggerRequest, request: Request):
    """!watch <login> from chat: pin one streamer onto the FetchClips watch list.
    Called by TwitchChatListenerProcessor (StreamersApp NiFi flow).

    The guards live here, not in streamers.add_to_watchlist() — that primitive is
    shared with LiveStreamerAlert, which pins a streamer it already resolved and
    already knows is live, and must keep its unconditional behaviour."""
    login = body.login.strip().lstrip("@").lower()
    if not login:
        raise HTTPException(status_code=400, detail="login required")
    platform = (body.platform or "twitch").strip().lower()
    entry = f"kick:{login}" if platform == "kick" else login

    current = streamers.get_watchlist()

    # Idempotency first: someone already on the list is a success no matter what
    # the cap or their live status says — re-asking must never be an error.
    if entry in current:
        return {"ok": True, "login": login, "added": False, "count": len(current),
                "message": f"{login} is already on the watch list"}

    http = request.app.state.http

    # Guard 1 — the channel has to actually exist. A typo must never make the
    # bot start watching (and clipping) a stranger.
    if not await streamers.streamer_exists(http, entry):
        return {"ok": False, "login": login, "added": False, "count": len(current),
                "reason": "unknown_channel",
                "message": f"can't find a {platform} channel called {login}"}

    # Guard 2 — live only, matching the !load precedent. It also means
    # WatchlistChatJoiner's existing offline auto-evict is what cleans these
    # chat-added entries back out; nothing has to remember to unpin them.
    if not await streamers.is_streamer_live(http, entry):
        return {"ok": False, "login": login, "added": False, "count": len(current),
                "reason": "not_live",
                "message": f"{login} isn't live right now — ask again when they are"}

    # Guard 3 — the cap. See _CHAT_WATCHLIST_MAX.
    if len(current) >= _CHAT_WATCHLIST_MAX:
        return {"ok": False, "login": login, "added": False, "count": len(current),
                "reason": "watchlist_full",
                "message": (f"watch list is full ({_CHAT_WATCHLIST_MAX}) — "
                            f"can't add {login} until someone drops off")}

    logins = streamers.add_to_watchlist(entry)
    return {"ok": True, "login": login, "added": True, "count": len(logins),
            "message": f"now watching {login} — clips are on the way"}


@router.post("/chat-trigger/clip")
async def chat_trigger_clip(body: ChatTriggerRequest, request: Request):
    """!clip <login> from chat: fetch one clip for that streamer and post it to X
    right now. Called by TwitchChatListenerProcessor, which enforces the mod-only
    gate before it ever calls this.

    This is the one trigger that posts straight to X with no human approval, so
    it runs the normal pipeline end to end — fetch → process_clip (Whisper +
    vLLM) → publish_clip — and honours every disqualification process_clip makes.
    It skips the review queue, not the quality gates."""
    login = body.login.strip().lstrip("@").lower()
    if not login:
        raise HTTPException(status_code=400, detail="login required")
    platform = (body.platform or "twitch").strip().lower()
    entry = f"kick:{login}" if platform == "kick" else login

    # A mod naming any streamer gets a clip on demand — no watchlist or live gate.
    # The watch list governs who the pipeline polls on its own, not who a mod may
    # pull one clip for by hand.

    # Gif-only streamers (clip=N in streamer_paths) never post the MP4, and the
    # .gif is cut by the separate ProcessClips gif branch that an on-demand
    # fetch doesn't run. Refuse rather than post the file approve_clip would
    # have refused to queue.
    if not streamers.streamer_paths(login)["clip"]:
        return {"ok": False, "login": login, "reason": "clip_posting_disabled",
                "message": f"{login}'s clips only go out as reaction GIFs, not video"}

    try:
        fetch = await streamers.fetch_clips_for_login(entry, clip_cap=1)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    records = fetch.get("records", [])
    if not records:
        errors = fetch.get("errors", [])
        failed = any("download" in e.lower() for e in errors)
        return {"ok": False, "login": login,
                "reason": "download_failed" if failed else "no_new_clips",
                "message": (f"couldn't grab a clip for {login} just now"
                            if failed else f"no new clips for {login} to post")}

    clip = records[0]
    try:
        processed = await streamers.process_clip(clip)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # process_clip returns its rejections in `error` rather than raising — no
    # transcript, a fabricated quote, a dead-black canvas. Posting anyway would
    # put out exactly what the normal pipeline exists to keep off the timeline.
    if processed.get("error"):
        return {"ok": False, "login": login, "clip_id": clip.get("clip_id", ""),
                "reason": processed["error"],
                "message": f"skipped that {login} clip: {processed['error']}"}

    # publish_clip raises on an X rejection rather than returning ok:false, and
    # the usual rejections (duplicate content, media too long, credentials) are
    # permanent — reported as ok:false/post_failed so InvokeHTTP doesn't retry a
    # post that will fail identically every time.
    try:
        result = await streamers.publish_clip(
            processed.get("clip_path", ""), processed.get("caption", ""),
            processed.get("clip_id", ""), processed.get("title", ""),
            processed.get("source", ""), processed.get("streamer", ""),
            processed.get("url", ""), processed.get("thumbnail_url", ""),
            streamers.get_x_handle(processed.get("streamer", "")),
        )
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    if not result.get("ok"):
        # Surface X's actual objection (#174) rather than a hardcoded
        # "post_failed" that discarded the captured error. Same clamp as the gif
        # path so a long tweepy/urllib3 error stays a single chat line.
        reason = result.get("error") or result.get("reason") or "post_failed"
        short = " ".join(str(reason).split())[:200]
        return {"ok": False, "login": login, "clip_id": clip.get("clip_id", ""),
                "reason": reason,
                "message": f"X wouldn't take the {login} clip — {short}"}

    return {
        "ok": True,
        "login": login,
        "clip_id": processed.get("clip_id", ""),
        "title": processed.get("title", ""),
        "stage": "posted",
        "tweet_url": result.get("url", ""),
        "message": f"posted a {login} clip: {result.get('url', '')}",
    }


@router.post("/chat-trigger/gif")
async def chat_trigger_gif(body: ChatTriggerRequest, request: Request):
    """🐟🐟🐟🖼️ <login> from chat: fetch one clip for that streamer, cut its
    reaction GIF, and post the GIF to X right now. Called by
    TwitchChatListenerProcessor, which enforces the mod-only gate before it ever
    calls this.

    The gif twin of /chat-trigger/clip — same fetch, but process_gif +
    publish_gif_now instead of process_clip + publish_clip, so it posts the .gif
    and never the MP4. A skipped gif (no confident face crop) is a normal
    outcome, reported as ok:false, not an error."""
    login = body.login.strip().lstrip("@").lower()
    if not login:
        raise HTTPException(status_code=400, detail="login required")
    platform = (body.platform or "twitch").strip().lower()
    entry = f"kick:{login}" if platform == "kick" else login

    # A mod naming any streamer gets a gif on demand — no watchlist or live gate.
    # Cutting a gif is just fetch → process, the same as a clip; the watch list is
    # about who the pipeline polls on its own, not who a mod may pull once by hand.

    # gif=N in streamer_paths means this streamer never gets a reaction GIF cut.
    if not streamers.streamer_paths(login)["gif"]:
        return {"ok": False, "login": login, "reason": "gif_posting_disabled",
                "message": f"{login} doesn't get reaction GIFs"}

    try:
        fetch = await streamers.fetch_clips_for_login(entry, clip_cap=1)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    records = fetch.get("records", [])
    if not records:
        errors = fetch.get("errors", [])
        failed = any("download" in e.lower() for e in errors)
        return {"ok": False, "login": login,
                "reason": "download_failed" if failed else "no_new_clips",
                "message": (f"couldn't grab a clip for {login} just now"
                            if failed else f"no new clips for {login} to gif")}

    clip = records[0]
    try:
        gif = await streamers.process_gif(clip)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    # process_gif reports a skip/failure in gif_error and leaves gif_path empty —
    # no confident face crop, a cut failure, or gif-off for this streamer.
    if not gif.get("gif_path") or gif.get("gif_error"):
        reason = gif.get("gif_error") or "no_gif"
        return {"ok": False, "login": login, "clip_id": clip.get("clip_id", ""),
                "reason": reason,
                "message": f"no gif for {login} this time: {reason}"}

    try:
        result = await streamers.publish_gif_now(clip.get("clip_id", ""))
    except Exception as e:
        result = {"ok": False, "reason": str(e)}

    if not result.get("ok"):
        # Surface X's actual objection (#174) - a generic "nothing posted" hid
        # why the extraemily gif failed and left it undiagnosable. Collapse
        # whitespace and clamp so a long tweepy/urllib3 error stays one chat line.
        reason = result.get("reason") or result.get("error") or "post_failed"
        short = " ".join(str(reason).split())[:200]
        return {"ok": False, "login": login, "clip_id": clip.get("clip_id", ""),
                "reason": reason,
                "message": f"X wouldn't take the {login} gif — {short}"}

    return {
        "ok": True,
        "login": login,
        "clip_id": clip.get("clip_id", ""),
        "stage": "posted",
        "tweet_url": result.get("url", ""),
        "message": f"posted a {login} gif: {result.get('url', '')}",
    }


# ── Fetch mode ────────────────────────────────────────────────────────────────

@router.get("/fetch-mode")
async def get_fetch_mode():
    return streamers.get_fetch_mode()


class FetchModeUpdate(BaseModel):
    mode: str   # "recent" | "top"
    period: str = "month"  # "month" | "all"


@router.post("/fetch-mode")
async def set_fetch_mode(body: FetchModeUpdate):
    return streamers.set_fetch_mode(body.mode, body.period)

import json
import os
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel

from config import settings
from services import streamers

router = APIRouter(prefix="/streamers")


# ── NiFi flows ───────────────────────────────────────────────────────────────

@router.get("/flows")
async def flows(request: Request):
    """NiFi status for FetchClips, ProcessClips, PublishClip."""
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
        return await streamers.flow_set_state(request.app.state.http, name, running=False)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


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
    try:
        return await streamers.publish_clip(
            body.clip_path, body.tweet_text, body.clip_id, body.title,
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


@router.get("/x-handle/{login}")
async def get_x_handle(login: str):
    """Passive catalog lookup for LiveStreamerAlert (NiFi) — X handle has no @, empty string if unknown."""
    return {"login": login, "x_handle": streamers.get_x_handle(login)}


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

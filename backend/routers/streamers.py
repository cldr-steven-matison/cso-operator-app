from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

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


@router.post("/publish")
async def publish(body: PublishRequest):
    """Upload clip to X and create a tweet. Requires X credentials in config."""
    import os
    if not body.clip_path or not body.tweet_text:
        raise HTTPException(status_code=400, detail="clip_path and tweet_text are required")
    if not os.path.exists(body.clip_path):
        raise HTTPException(status_code=404, detail=f"Clip file not found: {body.clip_path} — re-fetch clips first")
    try:
        return await streamers.publish_clip(body.clip_path, body.tweet_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


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
        import json
        clip = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Expected JSON clip metadata")
    result = await streamers.process_clip(clip)
    return result


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

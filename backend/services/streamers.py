"""Streamers module — Twitch clip pipeline backend services.

NiFi flows (FetchClips, ProcessClips) call back into this backend via HTTP so
the heavy lifting (Twitch API, file I/O, Whisper, vLLM, Kafka publish) stays
in Python. NiFi handles scheduling, Kafka consume/publish, and flow routing.
X publishing is called directly from the Review UI Approve button.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

from config import settings

STREAMER_PG_NAMES = ("FetchClips", "ProcessClips", "PublishClip")

_watchlist: list[str] = []
_twitch_token: str = ""
_twitch_token_expires: float = 0.0


def _init_watchlist():
    global _watchlist
    _watchlist = [l.strip() for l in settings.STREAMERS_WATCH_LIST.split(",") if l.strip()]


_init_watchlist()


def get_watchlist() -> list[str]:
    return list(_watchlist)


def set_watchlist(logins: list[str]):
    global _watchlist
    _watchlist = [l.strip() for l in logins if l.strip()]


# ── NiFi flow control ─────────────────────────────────────────────────────────

async def _children(client: httpx.AsyncClient, group_id: str) -> list[dict]:
    from services.nifi import _get
    data = await _get(client, f"/process-groups/{group_id}/process-groups")
    return data.get("processGroups", [])


async def _resolve_streamer_groups(client: httpx.AsyncClient) -> dict[str, dict]:
    out: dict[str, dict] = {}
    queue: list[tuple[str, int]] = [("root", 0)]
    visited: set[str] = set()
    while queue:
        gid, depth = queue.pop(0)
        if gid in visited or depth > 5:
            continue
        visited.add(gid)
        try:
            children = await _children(client, gid)
        except Exception:
            continue
        for pg in children:
            name = pg.get("component", {}).get("name")
            if name in STREAMER_PG_NAMES and name not in out:
                out[name] = {
                    "id": pg["id"],
                    "version": pg.get("revision", {}).get("version", 0),
                }
            queue.append((pg["id"], depth + 1))
        if len(out) == len(STREAMER_PG_NAMES):
            break
    return out


async def _pg_state(client: httpx.AsyncClient, pg_id: str) -> str:
    from services.nifi import _get
    try:
        data = await _get(client, f"/flow/process-groups/{pg_id}")
        procs = data.get("processGroupFlow", {}).get("flow", {}).get("processors", [])
        counts = {"RUNNING": 0, "STOPPED": 0, "INVALID": 0, "DISABLED": 0}
        for p in procs:
            s = p.get("component", {}).get("state") or p.get("status", {}).get(
                "aggregateSnapshot", {}
            ).get("runStatus")
            if s in counts:
                counts[s] += 1
        if counts["RUNNING"]:
            return "RUNNING"
        if counts["STOPPED"]:
            return "STOPPED"
        if counts["INVALID"]:
            return "INVALID"
        return "STOPPED"
    except Exception:
        return "UNKNOWN"


async def flows_state(client: httpx.AsyncClient) -> dict:
    try:
        groups = await _resolve_streamer_groups(client)
    except Exception:
        groups = {}

    states: dict[str, dict] = {}
    if groups:
        results = await asyncio.gather(
            *(_pg_state(client, pg["id"]) for pg in groups.values()),
            return_exceptions=True,
        )
        for (name, pg), st in zip(groups.items(), results):
            states[name] = {**pg, "state": st if isinstance(st, str) else "UNKNOWN"}

    for name in STREAMER_PG_NAMES:
        if name not in states:
            states[name] = {"id": None, "version": 0, "state": "NOT_INSTALLED"}

    return states


async def flow_set_state(client: httpx.AsyncClient, name: str, running: bool) -> dict:
    from services.nifi import _put
    if name not in STREAMER_PG_NAMES:
        raise ValueError(f"Unknown streamer flow '{name}'")
    groups = await _resolve_streamer_groups(client)
    if name not in groups:
        raise ValueError(f"Flow '{name}' not yet installed — run scripts/setup-streamers-flows.py first")
    pg = groups[name]
    body = {
        "id": pg["id"],
        "state": "RUNNING" if running else "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    return await _put(client, f"/flow/process-groups/{pg['id']}", body)


# ── Twitch API ────────────────────────────────────────────────────────────────

async def _twitch_token_refresh(client: httpx.AsyncClient) -> str:
    global _twitch_token, _twitch_token_expires
    if _twitch_token and time.time() < _twitch_token_expires - 60:
        return _twitch_token
    if not (settings.TWITCH_CLIENT_ID and settings.TWITCH_CLIENT_SECRET):
        raise RuntimeError("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET not configured")
    r = await client.post(
        "https://id.twitch.tv/oauth2/token",
        params={
            "client_id": settings.TWITCH_CLIENT_ID,
            "client_secret": settings.TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=10.0,
    )
    r.raise_for_status()
    data = r.json()
    _twitch_token = data["access_token"]
    _twitch_token_expires = time.time() + data.get("expires_in", 3600)
    return _twitch_token


def _twitch_headers(token: str) -> dict:
    return {
        "Client-ID": settings.TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }


async def _get_broadcaster_id(client: httpx.AsyncClient, token: str, login: str) -> str | None:
    r = await client.get(
        "https://api.twitch.tv/helix/users",
        params={"login": login},
        headers=_twitch_headers(token),
        timeout=10.0,
    )
    if r.status_code != 200:
        return None
    data = r.json().get("data", [])
    return data[0]["id"] if data else None


async def _get_clips(client: httpx.AsyncClient, token: str, broadcaster_id: str, since: datetime) -> list[dict]:
    r = await client.get(
        "https://api.twitch.tv/helix/clips",
        params={
            "broadcaster_id": broadcaster_id,
            "first": "5",
            "started_at": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        headers=_twitch_headers(token),
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    return r.json().get("data", [])


_TWITCH_WEB_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"  # public Twitch web player client

_GQL_CLIP_QUERY = """
query VideoAccessToken_Clip($slug: ID!) {
  clip(slug: $slug) {
    id
    videoQualities {
      quality
      sourceURL
    }
  }
}
"""


async def _gql_clip_mp4_url(client: httpx.AsyncClient, clip_id: str) -> str | None:
    """Use Twitch GQL to get the highest-quality direct MP4 URL for a clip.

    The old thumbnail→.mp4 trick stopped working in 2024 when Twitch migrated
    their clip CDN. GQL returns signed CloudFront sourceURLs that are directly
    downloadable.
    """
    try:
        r = await client.post(
            "https://gql.twitch.tv/gql",
            json={"query": _GQL_CLIP_QUERY, "variables": {"slug": clip_id}},
            headers={"Client-ID": _TWITCH_WEB_CLIENT_ID},
            timeout=10.0,
        )
        if r.status_code != 200:
            return None
        qualities = r.json()["data"]["clip"]["videoQualities"]
        if not qualities:
            return None
        # First entry is highest quality
        return qualities[0]["sourceURL"]
    except Exception:
        return None


async def _download_clip(client: httpx.AsyncClient, url: str, dest: Path) -> bool:
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        async with client.stream("GET", url, timeout=120.0) as r:
            r.raise_for_status()
            # Reject obvious non-video responses (JPEG magic or tiny HTML)
            first_chunk = b""
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes(65536):
                    if not first_chunk:
                        first_chunk = chunk
                        if first_chunk[:3] == b"\xff\xd8\xff":
                            # JPEG — wrong URL; bail out immediately
                            dest.unlink(missing_ok=True)
                            return False
                    f.write(chunk)
        return dest.exists() and dest.stat().st_size > 10_000
    except Exception:
        dest.unlink(missing_ok=True)
        return False


async def fetch_clips() -> dict:
    """Poll Twitch for clips from the watch list, download to PVC, publish to new_clips."""
    logins = get_watchlist()
    if not logins:
        return {"fetched": 0, "clips": [], "error": "Watch list is empty"}

    clip_dir = Path(settings.CLIP_STORAGE_PATH)
    seen_file = clip_dir / ".seen_clips.json"
    seen: set[str] = set()
    if seen_file.exists():
        try:
            seen = set(json.loads(seen_file.read_text()))
        except Exception:
            pass

    since = datetime.now(timezone.utc) - timedelta(hours=6)
    fetched: list[dict] = []
    errors: list[str] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        token = await _twitch_token_refresh(client)

        for login in logins:
            broadcaster_id = await _get_broadcaster_id(client, token, login)
            if not broadcaster_id:
                errors.append(f"Could not resolve broadcaster_id for {login}")
                continue
            clips = await _get_clips(client, token, broadcaster_id, since)
            for clip in clips:
                clip_id = clip.get("id", "")
                if not clip_id or clip_id in seen:
                    continue

                dest = clip_dir / f"{clip_id}.mp4"
                if not dest.exists():
                    mp4_url = await _gql_clip_mp4_url(client, clip_id)
                    if not mp4_url:
                        errors.append(f"No download URL for {clip_id}")
                        continue
                    ok = await _download_clip(client, mp4_url, dest)
                    if not ok:
                        errors.append(f"Download failed for {clip_id}")
                        continue

                thumb = clip.get("thumbnail_url", "")
                metadata = {
                    "clip_id": clip_id,
                    "streamer": login,
                    "broadcaster_id": broadcaster_id,
                    "title": clip.get("title", ""),
                    "url": clip.get("url", ""),
                    "thumbnail_url": thumb,
                    "duration": clip.get("duration", 0),
                    "created_at": clip.get("created_at", ""),
                    "clip_path": str(dest),
                    "view_count": clip.get("view_count", 0),
                }
                fetched.append(metadata)
                seen.add(clip_id)

    if fetched:
        await _publish_clips_to_kafka(fetched)
        seen_file.write_text(json.dumps(list(seen)))

    return {"fetched": len(fetched), "clips": [c["clip_id"] for c in fetched], "errors": errors}


async def _publish_clips_to_kafka(clips: list[dict]):
    from aiokafka import AIOKafkaProducer
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    await producer.start()
    try:
        for clip in clips:
            await producer.send(
                settings.NEW_CLIPS_TOPIC,
                json.dumps(clip).encode("utf-8"),
            )
        await producer.flush()
    finally:
        await producer.stop()


# ── ProcessClip — Whisper + vLLM ──────────────────────────────────────────────

async def process_clip(clip: dict) -> dict:
    """Transcribe clip audio with Whisper, generate caption with vLLM.
    Returns enriched clip dict ready to publish to processed_clips."""
    clip_path = clip.get("clip_path", "")
    if not clip_path or not Path(clip_path).exists():
        return {**clip, "transcript": "", "caption": "", "error": f"File not found: {clip_path}"}

    async with httpx.AsyncClient(verify=False, timeout=120.0) as client:
        # Whisper transcription
        transcript = ""
        try:
            with open(clip_path, "rb") as f:
                r = await client.post(
                    f"{settings.WHISPER_URL}/transcribe",
                    files={"file": (Path(clip_path).name, f, "video/mp4")},
                )
            if r.status_code == 200:
                transcript = r.json().get("text", "")
        except Exception as e:
            transcript = f"[transcription error: {e}]"

        # vLLM caption generation
        caption = ""
        if transcript and not transcript.startswith("["):
            try:
                prompt = (
                    f"You are a social media editor for a gaming clip account. "
                    f"Write one punchy, witty sentence (max 100 chars) reacting to this Twitch clip transcript. "
                    f"Clip: '{clip.get('title', '')}' by {clip.get('streamer', 'unknown')}. "
                    f"Transcript: {transcript[:500]}"
                )
                r = await client.post(
                    f"{settings.VLLM_URL}/v1/chat/completions",
                    json={
                        "model": settings.VLLM_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 100,
                        "temperature": 0.8,
                    },
                )
                if r.status_code == 200:
                    caption = r.json()["choices"][0]["message"]["content"].strip()
            except Exception as e:
                caption = f"[caption error: {e}]"

    return {**clip, "transcript": transcript, "caption": caption}


# ── Clip queue ────────────────────────────────────────────────────────────────

async def clip_queue(limit: int = 20) -> list[dict]:
    """Peek the last `limit` records from processed_clips.

    Uses manual partition assignment (no group coordinator) to avoid the
    aiokafka CancelledError that happens with subscribe() on low-traffic topics.
    """
    from aiokafka import AIOKafkaConsumer, TopicPartition

    topic = settings.PROCESSED_CLIPS_TOPIC
    clips: list[dict] = []
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        await consumer.start()
        # Manual assignment — no group coordinator needed
        partitions = [TopicPartition(topic, p) for p in range(3)]
        consumer.assign(partitions)
        end_offsets = await consumer.end_offsets(partitions)
        for tp in partitions:
            end = end_offsets.get(tp, 0)
            start = max(0, end - limit)
            consumer.seek(tp, start)

        deadline = asyncio.get_event_loop().time() + 3.0
        async for msg in consumer:
            try:
                record = json.loads(msg.value.decode("utf-8"))
                record["_offset"] = msg.offset
                record["_partition"] = msg.partition
                record["_ts"] = msg.timestamp
                clips.append(record)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
            if asyncio.get_event_loop().time() > deadline or len(clips) >= limit:
                break
    except Exception:
        pass
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass
    return clips[-limit:]


# ── X publish ─────────────────────────────────────────────────────────────────

def _publish_sync(clip_path: str, tweet_text: str) -> dict:
    import tweepy

    if not all([settings.X_API_KEY, settings.X_API_SECRET,
                settings.X_ACCESS_TOKEN, settings.X_ACCESS_TOKEN_SECRET]):
        raise RuntimeError("X API credentials not configured")

    path = Path(clip_path)
    if not path.exists():
        raise FileNotFoundError(f"Clip not found: {clip_path}")

    auth = tweepy.OAuth1UserHandler(
        settings.X_API_KEY,
        settings.X_API_SECRET,
        settings.X_ACCESS_TOKEN,
        settings.X_ACCESS_TOKEN_SECRET,
    )
    api_v1 = tweepy.API(auth)
    media = api_v1.media_upload(str(path), chunked=True)

    client = tweepy.Client(
        consumer_key=settings.X_API_KEY,
        consumer_secret=settings.X_API_SECRET,
        access_token=settings.X_ACCESS_TOKEN,
        access_token_secret=settings.X_ACCESS_TOKEN_SECRET,
    )
    response = client.create_tweet(text=tweet_text, media_ids=[media.media_id])
    tweet_id = response.data["id"]
    return {
        "ok": True,
        "tweet_id": tweet_id,
        "url": f"https://x.com/TunaStreetTest/status/{tweet_id}",
    }


async def publish_clip(clip_path: str, tweet_text: str) -> dict:
    return await asyncio.to_thread(_publish_sync, clip_path, tweet_text)

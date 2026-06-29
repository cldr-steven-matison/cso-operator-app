"""Streamers module — Twitch clip pipeline backend services.

NiFi flows (FetchClips, ProcessClips) call back into this backend via HTTP so
the heavy lifting (Twitch API, file I/O, Whisper, vLLM, Kafka publish) stays
in Python. NiFi handles scheduling, Kafka consume/publish, and flow routing.
X publishing is called directly from the Review UI Approve button.
"""

import asyncio
import glob
import json
import re
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from config import settings

STREAMER_PG_NAMES = ("FetchClips", "ProcessClips", "PublishClip")

_watchlist: list[str] = []
_twitch_token: str = ""
_twitch_token_expires: float = 0.0
_kick_token: str = ""
_kick_token_expires: float = 0.0

# NiFi group-ID cache — BFS is expensive; group IDs don't change until a re-import
_pg_cache: dict[str, dict] = {}
_pg_cache_ts: float = 0.0
_PG_CACHE_TTL = 300.0

# topic_stats cache — Kafka consumer lifecycle is ~10s per topic; avoid per-request
_topic_stats_cache: dict = {}
_topic_stats_ts: float = 0.0
_TOPIC_STATS_TTL = 30.0


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
    global _pg_cache, _pg_cache_ts
    now = time.time()
    if _pg_cache and now < _pg_cache_ts + _PG_CACHE_TTL:
        return _pg_cache

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

    if out:  # only cache on success; retry immediately if NiFi was unreachable
        _pg_cache = out
        _pg_cache_ts = now
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


# ── Watch list helpers ────────────────────────────────────────────────────────

def _parse_watch_entry(entry: str) -> tuple[str, str]:
    """Split 'kick:login' → ('kick', 'login'); bare name → ('twitch', name)."""
    if entry.startswith("kick:"):
        return "kick", entry[5:]
    return "twitch", entry


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
            "first": "20",
            "started_at": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        headers=_twitch_headers(token),
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    clips = r.json().get("data", [])
    # Skip anything under 45s — prefer clips closer to the 60s max
    return sorted(
        [c for c in clips if c.get("duration", 0) >= 45],
        key=lambda c: c.get("duration", 0),
        reverse=True,
    )


_TWITCH_WEB_CLIENT_ID = "kimne78kx3ncx6brgo4mv6wki5h1ko"  # public Twitch web player client

_GQL_CLIP_QUERY = """
query VideoAccessToken_Clip($slug: ID!) {
  clip(slug: $slug) {
    id
    videoQualities {
      quality
      sourceURL
    }
    playbackAccessToken(
      params: {
        platform: "web"
        playerBackend: "mediaplayer"
        playerType: "site"
      }
    ) {
      signature
      value
    }
  }
}
"""


async def _gql_clip_mp4_url(client: httpx.AsyncClient, clip_id: str) -> str | None:
    """Use Twitch GQL to get the highest-quality signed MP4 URL for a clip.

    The old thumbnail→.mp4 trick stopped working in 2024 when Twitch migrated
    clip CDN. GQL returns a playbackAccessToken (sig+value) that must be
    appended to the CloudFront sourceURL as query params.
    """
    from urllib.parse import quote
    try:
        r = await client.post(
            "https://gql.twitch.tv/gql",
            json={"query": _GQL_CLIP_QUERY, "variables": {"slug": clip_id}},
            headers={"Client-ID": _TWITCH_WEB_CLIENT_ID},
            timeout=10.0,
        )
        if r.status_code != 200:
            return None
        clip = r.json()["data"]["clip"]
        qualities = clip.get("videoQualities", [])
        token = clip.get("playbackAccessToken", {})
        if not qualities or not token:
            return None
        sig = token["signature"]
        tok = token["value"]
        # sourceURL requires sig+token to authenticate against CloudFront
        source_url = qualities[0]["sourceURL"]
        return f"{source_url}?sig={sig}&token={quote(tok)}"
    except Exception:
        return None


# ── Kick API ──────────────────────────────────────────────────────────────────

_KICK_TOKEN_URL = "https://id.kick.com/oauth/token"
_KICK_API_BASE = "https://api.kick.com/public/v1"
_KICK_WEB_CLIPS = "https://kick.com/api/v2/clips"
_KICK_BROWSER_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://kick.com/",
}


async def _kick_token_refresh(client: httpx.AsyncClient) -> str:
    global _kick_token, _kick_token_expires
    if _kick_token and time.time() < _kick_token_expires - 60:
        return _kick_token
    if not (settings.KICK_CLIENT_ID and settings.KICK_CLIENT_SECRET):
        raise RuntimeError("KICK_CLIENT_ID and KICK_CLIENT_SECRET not configured")
    r = await client.post(
        _KICK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.KICK_CLIENT_ID,
            "client_secret": settings.KICK_CLIENT_SECRET,
        },
        timeout=10.0,
    )
    r.raise_for_status()
    data = r.json()
    _kick_token = data["access_token"]
    _kick_token_expires = time.time() + data.get("expires_in", 3600)
    return _kick_token


def _kick_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _get_kick_broadcaster_id(client: httpx.AsyncClient, token: str, slug: str) -> int | None:
    r = await client.get(
        f"{_KICK_API_BASE}/channels",
        params={"slug": slug},
        headers=_kick_headers(token),
        timeout=10.0,
    )
    if r.status_code != 200:
        return None
    data = r.json().get("data", [])
    if not data:
        return None
    # API returns broadcaster_user_id as the numeric channel ID
    return data[0].get("broadcaster_user_id")


async def _get_kick_clips(client: httpx.AsyncClient, slug: str) -> list[dict]:
    r = await client.get(
        _KICK_WEB_CLIPS,
        params={"channel": slug, "sort": "date"},
        headers=_KICK_BROWSER_HEADERS,
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    clips = r.json().get("clips", [])
    return sorted(
        [c for c in clips if c.get("duration", 0) >= 45],
        key=lambda c: c.get("duration", 0),
        reverse=True,
    )



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


# ── Skip / publish persistence ────────────────────────────────────────────────

def _load_id_set(path: Path) -> set[str]:
    if path.exists():
        try:
            return set(json.loads(path.read_text()))
        except Exception:
            pass
    return set()


def _save_id_set(path: Path, ids: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(ids)))


def _skipped_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".skipped.json"


def _published_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".published.json"


def _pending_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".pending_publish.json"


def _load_pending() -> list[dict]:
    p = _pending_path()
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return []
    return []


def _save_pending(pending: list[dict]) -> None:
    _pending_path().parent.mkdir(parents=True, exist_ok=True)
    _pending_path().write_text(json.dumps(pending))


def mark_skipped(clip_id: str) -> None:
    ids = _load_id_set(_skipped_path())
    ids.add(clip_id)
    _save_id_set(_skipped_path(), ids)


def mark_published(clip_id: str) -> None:
    ids = _load_id_set(_published_path())
    ids.add(clip_id)
    _save_id_set(_published_path(), ids)


def get_skipped() -> set[str]:
    return _load_id_set(_skipped_path())


def get_published() -> set[str]:
    return _load_id_set(_published_path())


def approve_clip(clip_id: str, clip_path: str, tweet_text: str) -> dict:
    """Queue a clip for X publishing. Returns immediately — NiFi drains the queue."""
    pending = _load_pending()
    if not any(p["clip_id"] == clip_id for p in pending):
        pending.append({"clip_id": clip_id, "clip_path": clip_path, "tweet_text": tweet_text})
        _save_pending(pending)
    return {"queued": True, "clip_id": clip_id, "position": len(pending)}


async def publish_next() -> dict:
    """Pop the first pending clip and publish it to X. Called by NiFi timer every 2 min."""
    pending = _load_pending()
    if not pending:
        return {"published": False, "reason": "queue empty"}
    clip = pending[0]
    result = await publish_clip(clip["clip_path"], clip["tweet_text"], clip["clip_id"])
    _save_pending(pending[1:])
    return {**result, "queue_remaining": len(pending) - 1}


async def fetch_clips() -> dict:
    """Poll Twitch and Kick for clips from the watch list, download to PVC, publish to new_clips.

    Watch list entries are 'login' (Twitch) or 'kick:login' (Kick).
    """
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
        # Pre-fetch tokens so parallel tasks can share them
        twitch_entries = [e for e in logins if not e.startswith("kick:")]
        kick_entries = [e for e in logins if e.startswith("kick:")]

        twitch_token: str | None = None
        if twitch_entries:
            try:
                twitch_token = await _twitch_token_refresh(client)
            except RuntimeError as e:
                errors.append(str(e))

        async def _do_kick(entry: str) -> list[dict]:
            _, login = _parse_watch_entry(entry)
            entry_errors: list[str] = []
            result = await _fetch_kick_clips(
                client, login, clip_dir, seen, entry_errors,
                kick_token_getter=lambda: _kick_token_refresh(client),
            )
            errors.extend(entry_errors)
            return result

        async def _do_twitch(entry: str) -> list[dict]:
            if twitch_token is None:
                return []
            _, login = _parse_watch_entry(entry)
            entry_errors: list[str] = []
            result = await _fetch_twitch_clips(
                client, twitch_token, login, clip_dir, seen, since, entry_errors,
            )
            errors.extend(entry_errors)
            return result

        tasks = [_do_kick(e) for e in kick_entries] + [_do_twitch(e) for e in twitch_entries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                fetched.extend(r)
                for m in r:
                    seen.add(m["clip_id"])

    if fetched:
        await _publish_clips_to_kafka(fetched)
        seen_file.write_text(json.dumps(list(seen)))

    return {"fetched": len(fetched), "clips": [c["clip_id"] for c in fetched], "errors": errors}


async def _fetch_twitch_clips(
    client: httpx.AsyncClient,
    token: str,
    login: str,
    clip_dir: Path,
    seen: set[str],
    since: datetime,
    errors: list[str],
) -> list[dict]:
    broadcaster_id = await _get_broadcaster_id(client, token, login)
    if not broadcaster_id:
        errors.append(f"Twitch: could not resolve broadcaster_id for {login}")
        return []
    clips = await _get_clips(client, token, broadcaster_id, since)
    result: list[dict] = []
    for clip in clips:
        if len(result) >= 2:
            break
        clip_id = clip.get("id", "")
        if not clip_id or clip_id in seen:
            continue
        seen.add(clip_id)
        dest = clip_dir / f"{clip_id}.mp4"
        if not dest.exists():
            mp4_url = await _gql_clip_mp4_url(client, clip_id)
            if not mp4_url:
                errors.append(f"Twitch: no download URL for {clip_id}")
                continue
            ok = await _download_clip(client, mp4_url, dest)
            if not ok:
                errors.append(f"Twitch: download failed for {clip_id}")
                continue
        result.append({
            "clip_id": clip_id,
            "source": "twitch",
            "streamer": login,
            "broadcaster_id": broadcaster_id,
            "title": clip.get("title", ""),
            "url": clip.get("url", ""),
            "thumbnail_url": clip.get("thumbnail_url", ""),
            "duration": clip.get("duration", 0),
            "created_at": clip.get("created_at", ""),
            "clip_path": str(dest),
            "view_count": clip.get("view_count", 0),
        })
    return result


async def _fetch_kick_clips(
    client: httpx.AsyncClient,
    login: str,
    clip_dir: Path,
    seen: set[str],
    errors: list[str],
    kick_token_getter,
) -> list[dict]:
    clips = await _get_kick_clips(client, login)
    result: list[dict] = []
    for clip in clips:
        if len(result) >= 2:
            break
        raw_id = clip.get("id", "")
        if not raw_id:
            continue
        clip_id = f"kick_{raw_id.replace('-', '')}"
        if clip_id in seen:
            continue
        seen.add(clip_id)
        dest = clip_dir / f"{clip_id}.mp4"
        if not dest.exists():
            m3u8_url = clip.get("clip_url") or clip.get("video_url", "")
            if not m3u8_url:
                errors.append(f"Kick: no clip_url for {clip_id}")
                continue
            ok = await asyncio.get_event_loop().run_in_executor(
                None, lambda u=m3u8_url, d=dest: _download_hls_sync(u, d)
            )
            if not ok:
                errors.append(f"Kick: download failed for {clip_id}")
                continue
        result.append({
            "clip_id": clip_id,
            "source": "kick",
            "streamer": login,
            "title": clip.get("title", ""),
            "url": clip.get("clip_url", ""),
            "thumbnail_url": clip.get("thumbnail_url", ""),
            "duration": clip.get("duration", 0),
            "created_at": clip.get("created_at", ""),
            "clip_path": str(dest),
            "view_count": clip.get("view_count", 0),
        })
    return result


def _download_hls_sync(m3u8_url: str, dest: Path) -> bool:
    import subprocess
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", m3u8_url,
                "-c", "copy", "-movflags", "+faststart",
                str(dest),
            ],
            capture_output=True,
            timeout=120,
        )
        return result.returncode == 0 and dest.exists() and dest.stat().st_size > 10_000
    except Exception:
        dest.unlink(missing_ok=True)
        return False


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

def _clean_caption(text: str) -> str:
    """Strip model formatting artifacts from vLLM caption output.

    Small models often wrap the answer in a label like:
      **Punchy Reaction:** "actual tweet text"
    followed by self-commentary sections starting with — or —.
    This extracts just the bare tweet text.
    """
    text = text.strip()
    # Only the first paragraph is the caption; the rest is model commentary
    text = text.split("\n\n")[0].strip()
    # Strip leading label: **Word(s):** or "Word(s):"
    text = re.sub(r'^\*{0,2}[\w][\w ]*\*{0,2}:\s*', '', text)
    # Strip surrounding double-quotes that the model adds around the answer
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    # Normalize weird hashtags: #ALL_CAPS or #WORD_WORD → #TitleCase
    def _fix_tag(m: re.Match) -> str:
        tag = m.group(1)
        if '_' in tag:
            return '#' + ''.join(w.capitalize() for w in tag.split('_'))
        if tag.isupper() and len(tag) > 3:
            return '#' + tag.capitalize()
        return m.group(0)
    text = re.sub(r'#([A-Za-z_]+)', _fix_tag, text)
    return text.strip()


async def process_clip(clip: dict) -> dict:
    """Transcribe clip audio with Whisper, generate caption with vLLM.
    Returns enriched clip dict ready to publish to processed_clips."""
    clip_path = clip.get("clip_path", "")
    if not clip_path or not Path(clip_path).exists():
        return {**clip, "transcript": "", "caption": "", "error": f"File not found: {clip_path}"}

    async with httpx.AsyncClient(verify=False, timeout=120.0) as client:
        # Whisper transcription — extract 16kHz mono WAV first (soundfile in Whisper can't read MP4)
        transcript = ""
        wav_path = Path(clip_path).with_suffix(".wav")
        try:
            await asyncio.get_event_loop().run_in_executor(None, lambda: subprocess.run(
                ["ffmpeg", "-y", "-i", clip_path, "-vn", "-ac", "1", "-ar", "16000", str(wav_path)],
                capture_output=True, timeout=60,
            ))
            with open(wav_path, "rb") as f:
                r = await client.post(
                    f"{settings.WHISPER_URL}/transcribe",
                    files={"file": ("clip.wav", f, "audio/wav")},
                )
            if r.status_code == 200:
                transcript = r.json().get("text", "")
        except Exception as e:
            transcript = f"[transcription error: {e}]"
        finally:
            wav_path.unlink(missing_ok=True)

        # vLLM caption generation
        caption = ""
        if transcript and not transcript.startswith("["):
            try:
                r = await client.post(
                    f"{settings.VLLM_URL}/v1/chat/completions",
                    json={
                        "model": settings.VLLM_MODEL,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a hype social media editor. Output ONLY the tweet text — no labels, no headers, no explanation, no surrounding quotes.",
                            },
                            {
                                "role": "user",
                                "content": (
                                    f"Write one punchy, witty tweet reaction (max 220 chars) to this {clip.get('source', 'twitch').capitalize()} clip. "
                                    f"You MUST include the word '{'Kick' if clip.get('source') == 'kick' else 'Twitch'}' somewhere in the tweet. "
                                    f"Use 2-4 emojis and gaming slang naturally. "
                                    f"Examples: 'bro said what 💀', 'no way he actually did that 😭🔥', 'chat was NOT ready 👀'. "
                                    f"Clip: '{clip.get('title', '')}' by {clip.get('streamer', 'unknown')}. "
                                    f"Transcript: {transcript[:500]}"
                                ),
                            },
                        ],
                        "max_tokens": 100,
                        "temperature": 0.8,
                    },
                )
                if r.status_code == 200:
                    raw = r.json()["choices"][0]["message"]["content"]
                    caption = _clean_caption(raw)
            except Exception as e:
                caption = f"[caption error: {e}]"

    return {**clip, "transcript": transcript, "caption": caption}


# ── Clip queue ────────────────────────────────────────────────────────────────

async def clip_queue(limit: int = 20) -> list[dict]:
    """Peek the last `limit` records from processed_clips.

    Uses getmany() (single direct fetch) instead of the async iterator so
    that manual seek() works reliably — the async for iterator hangs after
    seek() in aiokafka when there are no in-flight fetch requests queued.
    """
    from aiokafka import AIOKafkaConsumer, TopicPartition

    topic = settings.PROCESSED_CLIPS_TOPIC
    clips: list[dict] = []
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        request_timeout_ms=10000,
    )
    try:
        await asyncio.wait_for(consumer.start(), timeout=10.0)
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])

        end_map = await asyncio.wait_for(consumer.end_offsets([tp]), timeout=10.0)
        end = end_map.get(tp, 0)
        if end == 0:
            return []

        consumer.seek(tp, max(0, end - limit))

        # getmany() is a one-shot fetch that respects the seek offset
        batch = await asyncio.wait_for(
            consumer.getmany(tp, timeout_ms=5000, max_records=limit),
            timeout=10.0,
        )
        skipped = get_skipped()
        published = get_published()
        for msg in batch.get(tp, []):
            try:
                record = json.loads(msg.value.decode("utf-8"))
                clip_id = record.get("clip_id", "")
                # Filter: missing file, already skipped, or already published
                clip_path = record.get("clip_path", "")
                if not clip_path or not Path(clip_path).exists():
                    continue
                if clip_id in skipped or clip_id in published:
                    continue
                record["_offset"] = msg.offset
                record["_partition"] = msg.partition
                record["_ts"] = msg.timestamp
                clips.append(record)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
    except Exception:
        pass
    finally:
        try:
            await asyncio.wait_for(consumer.stop(), timeout=5.0)
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


async def publish_clip(clip_path: str, tweet_text: str, clip_id: str = "") -> dict:
    result = await asyncio.to_thread(_publish_sync, clip_path, tweet_text)
    if result.get("ok") and clip_id:
        mark_published(clip_id)
    return result


# ── Topic stats ───────────────────────────────────────────────────────────────

async def _fetch_one_topic_stats(topic: str) -> dict:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
    )
    try:
        await asyncio.wait_for(consumer.start(), timeout=8.0)
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        begin_map = await asyncio.wait_for(consumer.beginning_offsets([tp]), timeout=5.0)
        end_map = await asyncio.wait_for(consumer.end_offsets([tp]), timeout=5.0)
        begin = begin_map.get(tp, 0)
        end = end_map.get(tp, 0)
        count = max(0, end - begin)

        records = []
        if count > 0:
            consumer.seek(tp, max(begin, end - 5))
            batch = await asyncio.wait_for(
                consumer.getmany(tp, timeout_ms=3000, max_records=5),
                timeout=8.0,
            )
            for msg in batch.get(tp, []):
                try:
                    rec = json.loads(msg.value.decode("utf-8"))
                    records.append({
                        "offset": msg.offset,
                        "source": rec.get("source", "twitch"),
                        "streamer": rec.get("streamer", ""),
                        "title": rec.get("title", ""),
                        "clip_id": rec.get("clip_id", ""),
                        "caption": rec.get("caption", ""),
                        "has_file": bool(rec.get("clip_path") and Path(rec["clip_path"]).exists()),
                    })
                except Exception:
                    pass
        return {"count": count, "records": records}
    except Exception as e:
        return {"count": 0, "records": [], "error": str(e)}
    finally:
        try:
            await asyncio.wait_for(consumer.stop(), timeout=5.0)
        except Exception:
            pass


async def topic_stats() -> dict:
    """Return message count and sample records for new_clips and processed_clips.

    Both consumers run in parallel and results are cached for 30s to avoid
    creating Kafka consumer lifecycles on every page refresh.
    """
    global _topic_stats_cache, _topic_stats_ts
    now = time.time()
    if _topic_stats_cache and now < _topic_stats_ts + _TOPIC_STATS_TTL:
        return _topic_stats_cache

    results = await asyncio.gather(
        _fetch_one_topic_stats(settings.NEW_CLIPS_TOPIC),
        _fetch_one_topic_stats(settings.PROCESSED_CLIPS_TOPIC),
        return_exceptions=True,
    )

    def _safe(r: object) -> dict:
        return r if not isinstance(r, Exception) else {"count": 0, "records": [], "error": str(r)}

    result = {
        "new_clips": _safe(results[0]),
        "processed_clips": _safe(results[1]),
    }
    _topic_stats_cache = result
    _topic_stats_ts = now
    return result


# ── Kafka reset ───────────────────────────────────────────────────────────────

async def reset_kafka() -> dict:
    """Delete new_clips and processed_clips topics directly via Kafka Admin API,
    then wipe the /clips directory. Topics auto-recreate when data next arrives."""
    from aiokafka.admin import AIOKafkaAdminClient
    from aiokafka.errors import UnknownTopicOrPartitionError

    # Clear clips on disk
    storage = Path(settings.CLIP_STORAGE_PATH)
    removed_files = 0
    for mp4 in glob.glob(str(storage / "*.mp4")):
        Path(mp4).unlink(missing_ok=True)
        removed_files += 1
    (storage / ".seen_clips.json").write_text("[]")
    (storage / ".skipped.json").write_text("[]")
    (storage / ".published.json").write_text("[]")
    (storage / ".pending_publish.json").write_text("[]")

    # Delete topics directly via Kafka Admin API — same as what Surveyor does
    deleted = []
    errors = []
    topics = [settings.NEW_CLIPS_TOPIC, settings.PROCESSED_CLIPS_TOPIC]
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    try:
        await asyncio.wait_for(admin.start(), timeout=10.0)
        results = await asyncio.wait_for(admin.delete_topics(topics, timeout_ms=10000), timeout=15.0)
        for topic, exc in results.items() if hasattr(results, "items") else []:
            if exc is None:
                deleted.append(topic)
            else:
                errors.append(f"{topic}: {exc}")
        if not deleted and not errors:
            deleted = topics
    except Exception as e:
        errors.append(str(e))
    finally:
        try:
            await asyncio.wait_for(admin.stop(), timeout=5.0)
        except Exception:
            pass

    return {
        "deleted_topics": deleted,
        "removed_clips": removed_files,
        "seen_clips_reset": True,
        "errors": errors,
    }

"""Streamers module — Twitch clip pipeline backend services.

NiFi flows (FetchClips, ProcessClips) call back into this backend via HTTP so
the heavy lifting (Twitch API, file I/O, Whisper, vLLM, Kafka publish) stays
in Python. NiFi handles scheduling, Kafka consume/publish, and flow routing.
X publishing is called directly from the Review UI Approve button.
"""

import asyncio
import contextlib
import fcntl
import glob
import json
import random
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from config import settings

STREAMER_PG_NAMES = ("FetchClips", "ProcessClips", "PublishClip")

_TWITCH_LOGINS: list[str] = [
    "xqc", "ishowspeed", "stableronaldo", "jynxzi", "agent00",
    "extraemily", "eliasn97", "hello_kiko", "theburntpeanut",
    "jasontheween", "zackrawrr", "lacy", "kaicenat",
]

_KICK_LOGINS: list[str] = [
    "roshtein", "deenthegreat", "hstikkytokky", "odablock",
    "iceposeidon", "adinross", "n3on",
    "chickenandy", "asmongold", "mrbeast", "clavicular",
]

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
    twitch_picks = random.sample(_TWITCH_LOGINS, min(2, len(_TWITCH_LOGINS)))
    kick_picks = [f"kick:{l}" for l in random.sample(_KICK_LOGINS, min(2, len(_KICK_LOGINS)))]
    _watchlist = twitch_picks + kick_picks


_init_watchlist()


def get_watchlist() -> list[str]:
    return list(_watchlist)


def set_watchlist(logins: list[str]):
    global _watchlist
    _watchlist = [l.strip() for l in logins if l.strip()]


def rotate_watchlist() -> list[str]:
    """Swap the current watch list for 4 new streamers (2 Twitch, 2 Kick) not already on it.

    Only updates in-memory state — FetchClips reads the watch list once at flow
    start, so the new picks take effect on the next stop/start of that PG.
    """
    global _watchlist
    current = set(_watchlist)
    twitch_pool = [l for l in _TWITCH_LOGINS if l not in current]
    kick_pool = [l for l in _KICK_LOGINS if f"kick:{l}" not in current]
    twitch_picks = random.sample(twitch_pool, min(2, len(twitch_pool)))
    kick_picks = [f"kick:{l}" for l in random.sample(kick_pool, min(2, len(kick_pool)))]
    _watchlist = twitch_picks + kick_picks
    return list(_watchlist)


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


async def _get_clips(
    client: httpx.AsyncClient,
    token: str,
    broadcaster_id: str,
    since: datetime | None,
    top_mode: bool = False,
) -> list[dict]:
    """Twitch's Helix /clips returns clips in recency order, not by views, and a single
    page tops out at 100. For top_mode we page through the whole window before ranking
    by view_count — otherwise "top clips" is really just "highest-viewed among the 20
    (or 100) most recent clips", missing anything popular from earlier in the window."""
    params: dict = {"broadcaster_id": broadcaster_id, "first": "100"}
    if since is not None:
        params["started_at"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_clips: list[dict] = []
    cursor: str | None = None
    max_pages = 5 if top_mode else 1
    for _ in range(max_pages):
        page_params = dict(params)
        if cursor:
            page_params["after"] = cursor
        r = await client.get(
            "https://api.twitch.tv/helix/clips",
            params=page_params,
            headers=_twitch_headers(token),
            timeout=10.0,
        )
        if r.status_code != 200:
            break
        body = r.json()
        all_clips.extend(body.get("data", []))
        cursor = body.get("pagination", {}).get("cursor")
        if not cursor:
            break

    valid = [c for c in all_clips if c.get("duration", 0) >= 45]
    if top_mode:
        return sorted(valid, key=lambda c: c.get("view_count", 0), reverse=True)
    return sorted(valid, key=lambda c: c.get("duration", 0), reverse=True)


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


async def _get_kick_clips(
    client: httpx.AsyncClient,
    slug: str,
    top_mode: bool = False,
    period: str = "week",
) -> list[dict]:
    # Kick channel endpoint only supports sort=date — no time window or views sort available.
    # Fetch 20 most recent clips and sort by view_count client-side.
    # top_mode/period are Twitch-only; Kick ignores them.
    r = await client.get(
        f"https://kick.com/api/v2/channels/{slug}/clips",
        headers=_KICK_BROWSER_HEADERS,
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    data = r.json()
    clips = data.get("clips", data.get("data", []))
    valid = [c for c in clips if 45 <= c.get("duration", 0) <= 90]
    return sorted(valid, key=lambda c: c.get("view_count", 0), reverse=True)



_ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
_OVERLAY_FONT = _ASSETS_DIR / "fonts" / "DejaVuSans-Bold.ttf"
_OVERLAY_LOGOS = {
    "kick": _ASSETS_DIR / "logos" / "kick.png",
    "twitch": _ASSETS_DIR / "logos" / "twitch.png",
}
_OVERLAY_DOMAINS = {"kick": "KICK.COM", "twitch": "TWITCH.TV"}
# Kick's cropped wordmark reads smaller than Twitch's at the same pixel height, so it gets a bigger ratio.
_OVERLAY_LOGO_HEIGHT_RATIO = {"kick": 0.09, "twitch": 0.1111}

# Re-encoding one clip at a time keeps CPU/memory bounded under the pod's
# 1 CPU / 1Gi limits — running several ffmpeg burns in parallel (one per
# streamer in fetch_clips' asyncio.gather, or an ad-hoc reprocessing script
# racing the live app) pegged both and risked an OOM kill. An in-memory
# threading.Semaphore only protects one process's own module state, so any
# separate process (a standalone script, a `kubectl exec` test) gets its own
# lock and doesn't see the live app's — use a flock on a shared file instead,
# which serializes across every process in the pod.
_OVERLAY_LOCK_PATH = Path("/tmp/.overlay_ffmpeg.lock")


@contextlib.contextmanager
def _overlay_lock():
    with open(_OVERLAY_LOCK_PATH, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _probe_video_dims(path: Path) -> tuple[int, int] | None:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height", "-of", "csv=p=0",
                str(path),
            ],
            capture_output=True, text=True, timeout=15,
        )
        w_str, h_str = result.stdout.strip().split(",")
        return int(w_str), int(h_str)
    except Exception:
        return None


def _burn_platform_overlay(dest: Path, source: str, streamer: str) -> int:
    """Add a top bar above the clip: platform logo on the left, PLATFORM.COM/HANDLE on the right.

    The bar extends the canvas (via `pad`) rather than compositing over the
    top of the frame, so none of the original footage is cropped or covered —
    the output is simply taller, which also makes the clip visibly distinct
    from a straight scrape rather than a copy with a stamp on it.

    Sizes are computed in literal pixels from the clip's actual resolution
    (via ffprobe) so the bar looks consistent whatever Twitch/Kick hands
    back. Deliberately avoids ffmpeg's `scale2ref` filter — it silently
    collapsed the small Twitch logo (151x51) down to ~9x5px instead of
    scaling it up, making the logo invisible; plain `scale` with pixel
    values sidesteps the bug entirely.

    Returns the bar height in pixels on success (0 if no bar was added), so
    the caller can pass it straight to _burn_glitch_intro without having to
    re-derive it from the now-taller clip.
    """
    logo_path = _OVERLAY_LOGOS.get(source)
    if not logo_path or not logo_path.exists():
        return 0
    dims = _probe_video_dims(dest)
    if not dims:
        return 0
    width, height = dims
    bar_h = round(height * 0.1481 / 2) * 2  # keep even — libx264 needs even dimensions
    new_height = height + bar_h
    logo_h = round(height * _OVERLAY_LOGO_HEIGHT_RATIO.get(source, 0.1111))
    font_size = round(height * 0.0519)
    left_pad = round(width * 0.0146)
    right_pad = round(width * 0.0208)
    label = f"{_OVERLAY_DOMAINS.get(source, source.upper())}/{streamer.upper()}"
    tmp = dest.with_suffix(".overlay.mp4")
    filter_complex = (
        f"[0:v]pad=width={width}:height={new_height}:x=0:y={bar_h}:color=black[padded];"
        f"[1:v]scale=-1:{logo_h}[logo];"
        f"[padded][logo]overlay=x={left_pad}:y=({bar_h}-overlay_h)/2[v1];"
        f"[v1]drawtext=fontfile={_OVERLAY_FONT}:text='{label}':fontcolor=white:"
        f"fontsize={font_size}:x=w-tw-{right_pad}:y=({bar_h}-th)/2[vout]"
    )
    try:
        with _overlay_lock():
            result = subprocess.run(
                [
                    "ffmpeg", "-y", "-threads", "1", "-i", str(dest), "-i", str(logo_path),
                    "-filter_complex", filter_complex,
                    "-map", "[vout]", "-map", "0:a?",
                    "-threads", "1", "-c:v", "libx264", "-preset", "veryfast",
                    "-x264opts", "threads=1:sliced-threads=0:rc-lookahead=20",
                    "-crf", "23", "-c:a", "copy", "-movflags", "+faststart",
                    str(tmp),
                ],
                capture_output=True,
                timeout=240,
            )
        if result.returncode != 0 or not tmp.exists() or tmp.stat().st_size < 10_000:
            tmp.unlink(missing_ok=True)
            return 0
        tmp.replace(dest)
        return bar_h
    except Exception:
        tmp.unlink(missing_ok=True)
        return 0


def _burn_glitch_intro(dest: Path, bar_h: int) -> bool:
    """Prepend a freeze-frame -> color-mosaic strobe -> hard-snap intro to a fresh clip.

    Only ever called immediately after download + overlay burn in
    _fetch_twitch_clips/_fetch_kick_clips — never re-applied to clips already sitting
    in /clips/, so existing videos are untouched.

    The platform bar (top bar_h pixels, already burned in by _burn_platform_overlay)
    is cropped out, held perfectly crisp for the whole intro, and stacked back on top
    so it never animates — only the footage below it freezes, pixelates, and strobes.
    Mosaic colors are sampled from the clip's own first frame, so every intro is
    unique to that clip rather than a generic effect. Hold/fade durations and which
    mosaic variants get used are randomized per clip so consecutive intros don't look
    identical.

    Every ffmpeg segment is encoded with zero B-frames (-bf 0). A version that used
    default B-frames crashed VLC on playback: stream-copy concatenating independently
    encoded B-frame segments produces DTS discontinuities at each splice that ffmpeg's
    own decoder tolerates (just a warning) but VLC's demuxer does not.
    """
    dims = _probe_video_dims(dest)
    if not dims:
        return False
    width, height = dims
    vid_h = height - bar_h
    if vid_h < 100:
        return False

    def run(args: list[str], timeout: int = 30) -> bool:
        try:
            r = subprocess.run(args, capture_output=True, timeout=timeout)
            if r.returncode != 0:
                print(f"[_burn_glitch_intro] ffmpeg failed ({dest.name}): {' '.join(args)}\n"
                      f"{r.stderr.decode(errors='replace')[-800:]}")
            return r.returncode == 0
        except Exception as e:
            print(f"[_burn_glitch_intro] ffmpeg exception ({dest.name}): {' '.join(args)} -> {e!r}")
            return False

    def encode_still(img: Path, dur: float, out: Path) -> bool:
        # -threads 1 + threads=1:sliced-threads=0 matches _burn_platform_overlay — without it,
        # libx264 auto-detects the *host's* full core count (seen: threads=24) instead of the
        # pod's 1-CPU cgroup limit, which produced silent zero-frame encodes under this pod's
        # actual constraints even though the same command worked fine on an unconstrained machine.
        return run([
            "ffmpeg", "-y", "-threads", "1", "-loop", "1", "-i", str(img), "-t", str(dur),
            "-vf", "format=yuv420p", "-r", "60", "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-threads", "1",
            "-bf", "0", "-x264opts", "threads=1:sliced-threads=0:scenecut=0", str(out),
        ], timeout=30)

    try:
        with tempfile.TemporaryDirectory(prefix="glitch_") as tmpdir, _overlay_lock():
            tmp = Path(tmpdir)
            frame0, vid0, bar0 = tmp / "frame0.png", tmp / "vid0.png", tmp / "bar0.png"

            if not run(["ffmpeg", "-y", "-i", str(dest), "-frames:v", "1", str(frame0)]):
                return False
            if bar_h > 0 and not run(["ffmpeg", "-y", "-i", str(frame0), "-vf",
                                       f"crop={width}:{bar_h}:0:0", str(bar0)]):
                return False
            if not run(["ffmpeg", "-y", "-i", str(frame0), "-vf",
                        f"crop={width}:{vid_h}:0:{bar_h}", str(vid0)]):
                return False

            # Randomized per-clip mosaic grids (denser = more visible color detail)
            base_cols = random.randint(48, 60)
            grids = [base_cols, round(base_cols * 0.85), round(base_cols * 1.2)]
            mosaics = []
            for i, cols in enumerate(grids):
                rows = max(1, round(cols * vid_h / width))
                out = tmp / f"grid_{i}.png"
                if not run(["ffmpeg", "-y", "-i", str(vid0), "-vf",
                            f"scale={cols}:{rows}:flags=neighbor,scale={width}:{vid_h}:flags=neighbor",
                            str(out)]):
                    return False
                mosaics.append(out)

            primary = mosaics[0]
            flip_variants = []
            for j, flip in enumerate(["hflip", "vflip", "hflip,vflip"]):
                out = tmp / f"grid_flip_{j}.png"
                if not run(["ffmpeg", "-y", "-i", str(primary), "-vf", flip, str(out)]):
                    return False
                flip_variants.append(out)
            flash = tmp / "flash.png"
            run(["ffmpeg", "-y", "-i", str(primary), "-vf",
                 "eq=brightness=0.45:contrast=1.3", str(flash)])

            variant_pool = mosaics + flip_variants + [flash]
            random.shuffle(variant_pool)
            strobe_variants = variant_pool[:6]

            hold_dur = round(random.uniform(0.8, 1.3), 2)
            fade_dur = round(random.uniform(1.1, 1.5), 2)

            hold_mp4 = tmp / "hold.mp4"
            if not encode_still(vid0, hold_dur, hold_mp4):
                return False

            strobe_step = round(fade_dur / len(strobe_variants) + 0.02, 2)
            strobe_parts = []
            for k, img in enumerate(strobe_variants):
                out = tmp / f"strobe_{k}.mp4"
                if not encode_still(img, strobe_step, out):
                    return False
                strobe_parts.append(out)
            strobe_list = tmp / "strobe_list.txt"
            strobe_list.write_text("\n".join(f"file '{p}'" for p in strobe_parts))
            strobe_seq = tmp / "strobe_seq.mp4"
            if not run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(strobe_list),
                        "-c", "copy", str(strobe_seq)]):
                return False

            crisp_clip = tmp / "crisp.mp4"
            if not encode_still(vid0, fade_dur + 0.2, crisp_clip):
                return False
            fadeflash = tmp / "fadeflash.mp4"
            if not run(["ffmpeg", "-y", "-threads", "1", "-i", str(crisp_clip), "-i", str(strobe_seq),
                        "-filter_complex",
                        f"[0:v][1:v]xfade=transition=fade:duration={fade_dur}:offset=0[v]",
                        "-map", "[v]", "-an", "-c:v", "libx264", "-preset", "veryfast", "-threads", "1",
                        "-crf", "20", "-bf", "0", "-x264opts", "threads=1:sliced-threads=0:scenecut=0",
                        str(fadeflash)]):
                return False

            region_list = tmp / "region_list.txt"
            region_list.write_text(f"file '{hold_mp4}'\nfile '{fadeflash}'")
            region_mp4 = tmp / "region.mp4"
            if not run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(region_list),
                        "-c", "copy", str(region_mp4)]):
                return False

            if bar_h > 0:
                total_dur = hold_dur + fade_dur
                bar_loop = tmp / "bar_loop.mp4"
                if not encode_still(bar0, total_dur, bar_loop):
                    return False
                intro_noaudio = tmp / "intro_noaudio.mp4"
                if not run(["ffmpeg", "-y", "-threads", "1", "-i", str(bar_loop), "-i", str(region_mp4),
                            "-filter_complex", "[0:v][1:v]vstack=inputs=2[v]", "-map", "[v]",
                            "-an", "-c:v", "libx264", "-preset", "veryfast", "-threads", "1", "-crf", "20",
                            "-bf", "0", "-x264opts", "threads=1:sliced-threads=0:scenecut=0",
                            str(intro_noaudio)]):
                    return False
            else:
                intro_noaudio = region_mp4

            intro_full = tmp / "intro_full.mp4"
            if not run(["ffmpeg", "-y", "-i", str(intro_noaudio), "-f", "lavfi",
                        "-i", "anullsrc=r=48000:cl=stereo", "-c:v", "copy", "-c:a", "aac",
                        "-shortest", str(intro_full)]):
                return False

            final_list = tmp / "final_list.txt"
            final_list.write_text(f"file '{intro_full}'\nfile '{dest.resolve()}'")
            out_final = dest.with_suffix(".glitch.mp4")
            if not run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(final_list),
                        "-c", "copy", str(out_final)], timeout=60):
                return False
            if not out_final.exists() or out_final.stat().st_size < 10_000:
                out_final.unlink(missing_ok=True)
                return False
            out_final.replace(dest)
            return True
    except Exception:
        return False


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


def _fetch_mode_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".fetch_mode.json"


def get_fetch_mode() -> dict:
    p = _fetch_mode_path()
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {"mode": "recent", "period": "week"}


def set_fetch_mode(mode: str, period: str) -> dict:
    data = {"mode": mode, "period": period}
    _fetch_mode_path().parent.mkdir(parents=True, exist_ok=True)
    _fetch_mode_path().write_text(json.dumps(data))
    return data


# Catalog: bare login (lowercase) → X handle (no @ prefix).
# Source of truth: DesktopShare/streamers.md — keep both in sync when adding streamers.
_STREAMER_CATALOG: dict[str, str] = {
    # Twitch
    "xqc":            "xQc",
    "ishowspeed":     "ishowspeedsui",
    "stableronaldo":  "StableRonaldo",
    "jynxzi":         "jynxzi",
    "agent00":        "CallMeAgent00",
    "extraemily":     "ExtraEmilyy",
    "eliasn97":       "EliasN97",
    "hello_kiko":     "hello_kiko",
    "theburntpeanut": "theburntpeanut",
    "jasontheween":   "jasontheween",
    "zackrawrr":      "zackrawrr",
    "lacy":           "LacyHimself",
    "kaicenat":       "KaiCenat",
    # Kick
    "roshtein":       "roshtein",
    "deenthegreat":   "DeenTheGreat",
    "hstikkytokky":   "HSTikkyTokky",
    "odablock":       "Odablock",
    "iceposeidon":    "REALIcePoseidon",
    "adinross":       "adinross",
    "n3on":           "N3on",
    "chickenandy":    "ChickenAndy_",
    "asmongold":      "asmongold",
    "mrbeast":        "mrbeast",
    "clavicular":     "Clavicular0",
}

def get_x_handle(login: str) -> str:
    """Return X handle (no @) for a bare login, or empty string if not in catalog."""
    return _STREAMER_CATALOG.get(login.lower(), "")


_PENDING_LOCK_PATH = Path("/tmp/.pending_publish.lock")


@contextlib.contextmanager
def _pending_lock():
    with open(_PENDING_LOCK_PATH, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


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


def approve_clip(clip_id: str, clip_path: str, tweet_text: str, title: str = "") -> dict:
    """Queue a clip for X publishing. Returns immediately — NiFi drains the queue.

    Locked read-modify-write: without this, two near-simultaneous approvals (or an
    approval racing publish_next) can each read the same pending list and overwrite
    each other's append, silently dropping an approved clip from the queue.
    """
    with _pending_lock():
        pending = _load_pending()
        if not any(p["clip_id"] == clip_id for p in pending):
            pending.append({"clip_id": clip_id, "clip_path": clip_path, "tweet_text": tweet_text, "title": title})
            _save_pending(pending)
    return {"queued": True, "clip_id": clip_id, "position": len(pending)}


async def publish_next() -> dict:
    """Pop the first pending clip and publish it to X. Called by NiFi timer every 2 min.

    The pop-and-save happens under the same lock as approve_clip/cancel_pending so the
    queue stays strict FIFO even if NiFi fires overlapping calls (e.g. a slow upload
    causing the next GenerateFlowFile tick to overlap the previous InvokeHTTP). The
    slow network publish itself runs outside the lock so it doesn't block approvals.
    """
    with _pending_lock():
        pending = _load_pending()
        if not pending:
            return {"published": False, "reason": "queue empty"}
        clip = pending[0]
        _save_pending(pending[1:])
    result = await publish_clip(clip["clip_path"], clip["tweet_text"], clip["clip_id"], clip.get("title", ""))
    return {**result, "queue_remaining": len(pending) - 1}


def get_pending() -> list[dict]:
    """List clips queued for X publish, in post order."""
    return _load_pending()


def cancel_pending(clip_id: str) -> dict:
    """Remove a clip from the publish queue before NiFi drains it."""
    with _pending_lock():
        pending = _load_pending()
        remaining = [p for p in pending if p["clip_id"] != clip_id]
        if len(remaining) == len(pending):
            return {"ok": False, "clip_id": clip_id, "reason": "not in queue"}
        _save_pending(remaining)
    return {"ok": True, "clip_id": clip_id}


def _clips_per_streamer_cap(num_streamers: int) -> int:
    """Scale clips-per-streamer down as the watch list grows, to keep total fetch volume reasonable."""
    if num_streamers == 1:
        return 5
    if num_streamers == 2:
        return 3
    if num_streamers == 3:
        return 2
    return 1


async def fetch_clips() -> dict:
    """Poll Twitch and Kick for clips from the watch list, download to PVC, publish to new_clips.

    Watch list entries are 'login' (Twitch) or 'kick:login' (Kick).
    """
    logins = get_watchlist()
    if not logins:
        return {"fetched": 0, "clips": [], "error": "Watch list is empty"}

    clip_cap = _clips_per_streamer_cap(len(logins))

    clip_dir = Path(settings.CLIP_STORAGE_PATH)
    seen_file = clip_dir / ".seen_clips.json"
    seen: set[str] = set()
    if seen_file.exists():
        try:
            seen = set(json.loads(seen_file.read_text()))
        except Exception:
            pass

    fm = get_fetch_mode()
    top_mode = fm.get("mode") == "top"
    period = fm.get("period", "week")

    if top_mode and period == "all":
        since = None
    elif top_mode:
        since = datetime.now(timezone.utc) - timedelta(days=30)
    else:
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
                top_mode=top_mode, period=period, clip_cap=clip_cap,
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
                top_mode=top_mode, clip_cap=clip_cap,
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
    since: datetime | None,
    errors: list[str],
    top_mode: bool = False,
    clip_cap: int = 2,
) -> list[dict]:
    broadcaster_id = await _get_broadcaster_id(client, token, login)
    if not broadcaster_id:
        errors.append(f"Twitch: could not resolve broadcaster_id for {login}")
        return []
    clips = await _get_clips(client, token, broadcaster_id, since, top_mode=top_mode)
    result: list[dict] = []
    for clip in clips:
        if len(result) >= clip_cap:
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
            bar_h = await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, l=login: _burn_platform_overlay(d, "twitch", l)
            )
            await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, b=bar_h: _burn_glitch_intro(d, b)
            )
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
    top_mode: bool = False,
    period: str = "week",
    clip_cap: int = 2,
) -> list[dict]:
    clips = await _get_kick_clips(client, login, top_mode=top_mode, period=period)
    result: list[dict] = []
    for clip in clips:
        if len(result) >= clip_cap:
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
            bar_h = await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, l=login: _burn_platform_overlay(d, "kick", l)
            )
            await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, b=bar_h: _burn_glitch_intro(d, b)
            )
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

_EMOJI_RE = re.compile(
    "[\U0001F1E0-\U0001F1FF"
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0000FE00-\U0000FE0F"
    "]+",
    flags=re.UNICODE,
)


def _clean_caption(text: str) -> str:
    """Strip model formatting artifacts from vLLM caption output."""
    text = text.strip()
    # Only the first paragraph is the caption; the rest is model commentary
    text = text.split("\n\n")[0].strip()
    # Strip leading label: **Word(s):** or "Word(s):"
    text = re.sub(r'^\*{0,2}[\w][\w ]*\*{0,2}:\s*', '', text)
    # Strip surrounding double-quotes that the model adds around the answer
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    # Strip all hashtags — platform/handle suffix is added by _build_tweet
    text = re.sub(r'\s*#\w+', '', text)
    # Cap emojis — if model spammed more than 3 total, strip them all
    emoji_matches = _EMOJI_RE.findall(text)
    if sum(len(m) for m in emoji_matches) > 3:
        text = _EMOJI_RE.sub("", text)
    return text.strip()


def _build_tweet(caption: str, source: str, streamer: str, x_handle: str = "") -> str:
    """Assemble final tweet: reaction body + attribution line. Always ≤ 280 chars.

    Format with X handle:    '{reaction}\n\nTwitch streaming stableronaldo find me on X @StableRonaldo'
    Format without X handle: '{reaction}\n\nTwitch streaming stableronaldo'
    """
    platform = "Kick" if source == "kick" else "Twitch"
    if x_handle:
        suffix = f"\n\n{platform} streaming {streamer} find me on X @{x_handle}"
    else:
        suffix = f"\n\n{platform} streaming {streamer}"
    max_body = 280 - len(suffix)
    body = caption.strip()
    if len(body) > max_body:
        body = body[:max_body - 1].rstrip() + "…"
    return body + suffix


async def process_clip(clip: dict) -> dict:
    """Transcribe clip audio with Whisper, generate caption with vLLM.
    Returns enriched clip dict ready to publish to processed_clips."""
    clip_path = clip.get("clip_path", "")
    if not clip_path or not Path(clip_path).exists():
        return {**clip, "transcript": "", "caption": "", "error": f"File not found: {clip_path}"}

    # Disqualify clips with no usable title — vLLM has nothing to work with
    if len(clip.get("title", "").strip()) < 2:
        return {**clip, "transcript": "", "caption": "", "error": "disqualified: no title"}

    async with httpx.AsyncClient(verify=False, timeout=300.0) as client:
        # Extract 16kHz mono WAV — much smaller upload to Whisper than raw MP4
        transcript = ""
        wav_path = Path(clip_path).with_suffix(".wav")
        try:
            proc = await asyncio.to_thread(
                subprocess.run,
                ["ffmpeg", "-y", "-i", clip_path, "-vn", "-ac", "1", "-ar", "16000", str(wav_path)],
                capture_output=True,
                timeout=60,
            )
            if proc.returncode != 0 or not wav_path.exists():
                raise RuntimeError(f"ffmpeg failed: {proc.stderr.decode()[:200]}")
            with open(wav_path, "rb") as f:
                r = await client.post(
                    f"{settings.WHISPER_URL}/transcribe",
                    files={"file": ("clip.wav", f, "audio/wav")},
                )
            if r.status_code == 200:
                transcript = r.json().get("text", "").strip()
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
                                "content": "You are a hype gaming content creator writing tweets. Output ONLY the tweet text — no labels, no quotes around it.",
                            },
                            {
                                "role": "user",
                                "content": (
                                    f"React to this clip by {clip.get('streamer', 'unknown')} like you're live in their Twitch chat. "
                                    f"Talk directly to the streamer, quote the wildest line, or just react like a viewer who can't believe what they just saw. "
                                    f"Stay grounded in the transcript. Do not invent facts. Under 200 chars. Exactly 1 emoji. No hashtags. "
                                    f"Clip title: '{clip.get('title', '')}'. "
                                    f"Transcript: {transcript[:600]}"
                                ),
                            },
                        ],
                        "max_tokens": 120,
                        "temperature": 0.85,
                    },
                )
                if r.status_code == 200:
                    raw = r.json()["choices"][0]["message"]["content"]
                    caption = _clean_caption(raw)
                    x_handle = get_x_handle(clip.get("streamer", ""))
                    caption = _build_tweet(caption, clip.get("source", "twitch"), clip.get("streamer", ""), x_handle)
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
        pending = {p["clip_id"] for p in _load_pending()}
        for msg in batch.get(tp, []):
            try:
                record = json.loads(msg.value.decode("utf-8"))
                clip_id = record.get("clip_id", "")
                # Filter: missing file, skipped, pending, published, or disqualified/errored
                clip_path = record.get("clip_path", "")
                if not clip_path or not Path(clip_path).exists():
                    continue
                if clip_id in skipped or clip_id in published or clip_id in pending:
                    continue
                caption = record.get("caption", "")
                if not caption or caption.startswith("["):
                    continue
                record["_offset"] = msg.offset
                record["_partition"] = msg.partition
                record["_ts"] = msg.timestamp
                record["x_handle"] = get_x_handle(record.get("streamer", ""))
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


async def publish_clip(clip_path: str, tweet_text: str, clip_id: str = "", title: str = "") -> dict:
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

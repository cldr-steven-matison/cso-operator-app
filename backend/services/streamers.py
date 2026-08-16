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
import html
import json
import logging
import os
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
from PIL import Image, ImageFont

from config import settings

logger = logging.getLogger(__name__)

STREAMER_PG_NAMES = ("FetchClips", "ProcessClips", "PublishClipOffPeakDay", "PublishClipPeakTimeCron")

# LiveStreamerAlert's PollTimer (GenerateFlowFile) is CRON_DRIVEN and RUNNING as
# its normal resting state (0 0/30 20-23,0-3 * * ? as of 2026-07-23 — Steven's
# own live tuning, real EDT evening hours). A dedicated ManualPollTrigger was
# tried alongside it (2026-07-23) so Telegram could pulse a run without ever
# touching PollTimer's schedule, but starting the whole PG with it in place blew
# the flow up — Steven removed it same day. Telegram's run-once is back to
# pulsing PollTimer directly; the state-preserving restore in
# run_live_streamer_alert_once() (remembers RUNNING/STOPPED on entry, restores
# it after) is what keeps that from disturbing the cron schedule. Not in
# STREAMER_PG_NAMES: this is a processor-level pulse, not a whole-PG toggle.
LIVE_STREAMER_ALERT_PG_NAME = "LiveStreamerAlert"
LIVE_STREAMER_ALERT_POLL_PROCESSOR = "PollTimer"

# X rejects video posts longer than 120s on this account's tier ("This user is
# not allowed to post a video longer than 2 minutes"); trim with a safety margin
# rather than trust upstream duration fields, which aren't re-checked after the
# glitch intro is burned on.
MAX_TWEET_VIDEO_DURATION = 115.0

# Fetch-time inclusion window (seconds) — a platform-reported clip outside this
# range for its streamer is skipped before download. Defaults match each
# platform's existing house style; per-streamer overrides exist for streamers
# whose real clip mix doesn't fit it (confirmed live 2026-07-29: bbjess's Kick
# clips run 15-180s+, so the 45-90s default was silently dropping most of her
# catalog, including some that had already made it all the way through
# transcription/captioning on an earlier attempt).
_DEFAULT_KICK_CLIP_RANGE = (45.0, 90.0)
_DEFAULT_TWITCH_CLIP_RANGE = (45.0, 100.0)
_CLIP_DURATION_OVERRIDES: dict[str, tuple[float, float]] = {
    "bbjess": (20.0, 180.0),
}


def _clip_duration_range(login: str, default: tuple[float, float]) -> tuple[float, float]:
    return _CLIP_DURATION_OVERRIDES.get(login.lower(), default)


_TWITCH_LOGINS: list[str] = [
    "xqc", "stableronaldo", "jynxzi",
    "extraemily", "theburntpeanut",
    "jasontheween", "lacy", "kaicenat",
]

_KICK_LOGINS: list[str] = [
    "roshtein", "ac7ionman",
    "adinross", "n3on",
    "clavicular",
    "bbjess", "whiz", "rampagejackson",
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


def _atomic_write_json(path: Path, data) -> None:
    """Write JSON to `path` via a same-directory temp file + os.replace(), never
    a plain write_text(). write_text() isn't atomic -- a crash mid-write (this
    pod's ffmpeg calls are confirmed OOM-killable) can leave a torn/truncated
    file, and every loader in this module treats a JSON-parse failure as empty
    state, not an error, so a torn .pending_publish.json/.watchlist.json/etc.
    silently reads back as "nothing here" instead of surfacing anything wrong.
    os.replace() is atomic on the same filesystem, so a reader never observes a
    half-written file. Flagged in the 2026-07-17 review, proposed then, applied
    now. Orthogonal to _pending_lock/_overlay_lock (those serialize concurrent
    writers; this protects a single writer against its own crash mid-write) --
    callers that already take one of those locks should keep doing so."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(data))
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp_name)
        raise


def _watchlist_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".watchlist.json"


def _save_watchlist():
    _atomic_write_json(_watchlist_path(), _watchlist)


def _init_watchlist():
    """Load the persisted watch list (survives pod restarts/redeploys) or, on
    first-ever run with nothing saved yet, seed a random starting pick."""
    global _watchlist
    p = _watchlist_path()
    if p.exists():
        try:
            _watchlist = json.loads(p.read_text())
            return
        except Exception:
            pass
    twitch_picks = random.sample(_TWITCH_LOGINS, min(2, len(_TWITCH_LOGINS)))
    kick_picks = [f"kick:{l}" for l in random.sample(_KICK_LOGINS, min(2, len(_KICK_LOGINS)))]
    _watchlist = twitch_picks + kick_picks
    _save_watchlist()


_init_watchlist()


def get_watchlist() -> list[str]:
    return list(_watchlist)


def get_roster() -> list[str]:
    """Every catalog streamer (not just the watch list), same login/kick:login shape.

    For LiveStreamerAlert to poll the whole roster for live status instead of
    just the 4-entry watch list -- the watch list still drives FetchClips.
    """
    return _TWITCH_LOGINS + [f"kick:{l}" for l in _KICK_LOGINS]


def set_watchlist(logins: list[str]):
    global _watchlist
    _watchlist = [l.strip() for l in logins if l.strip()]
    _save_watchlist()


def add_to_watchlist(login: str) -> list[str]:
    """Append one login (already-normalized, e.g. 'kick:n3on') if not already present.

    Unlike set_watchlist/rotate_watchlist this doesn't cap or replace — meant for
    LiveStreamerAlert to pin a streamer it just found live without disturbing the
    rest of the list.
    """
    global _watchlist
    login = login.strip()
    if login and login not in _watchlist:
        _watchlist.append(login)
        _save_watchlist()
    return list(_watchlist)


def remove_from_watchlist(login: str) -> list[str]:
    """Drop one login (already-normalized, e.g. 'kick:n3on') if present, leaving
    the rest of the list untouched — the offline-side counterpart to
    add_to_watchlist(), for per-streamer flows (e.g. tunastarlink's dedicated
    live-check) that pin on live and unpin on offline.
    """
    global _watchlist
    login = login.strip()
    if login in _watchlist:
        _watchlist.remove(login)
        _save_watchlist()
    return list(_watchlist)


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
    _save_watchlist()
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


async def _fetch_clips_state(client: httpx.AsyncClient, pg_id: str) -> str:
    """FetchClips' Stop only stops its GenerateFlowFile timer (see
    stop_fetch_clips_generator), not the whole PG -- so "state" here has to
    mean that processor's state specifically, not the generic any-processor-
    running heuristic _pg_state() uses (which would still say RUNNING with
    InvokeHTTP up and the timer stopped, making a successful Stop look like
    it did nothing)."""
    from services.nifi import _get
    try:
        data = await _get(client, f"/process-groups/{pg_id}/processors")
        for p in data.get("processors", []):
            if p.get("component", {}).get("name") == "GenerateFlowFile":
                return p["component"].get("state", "UNKNOWN")
        return "UNKNOWN"
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
            *(
                _fetch_clips_state(client, pg["id"]) if name == "FetchClips"
                else _pg_state(client, pg["id"])
                for name, pg in groups.items()
            ),
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


async def stop_fetch_clips_generator(client: httpx.AsyncClient) -> dict:
    """Stop only FetchClips' GenerateFlowFile head processor, leaving the rest of
    the PG (InvokeHTTP etc.) running so an in-flight fetch isn't cut off mid-run.
    Telegram's 'stop' asymmetric with 'start', which still starts the whole PG."""
    groups = await _resolve_streamer_groups(client)
    if "FetchClips" not in groups:
        raise ValueError("Flow 'FetchClips' not yet installed — run scripts/setup-streamers-flows.py first")
    pg_id = groups["FetchClips"]["id"]
    processor = await _find_processor(client, pg_id, "GenerateFlowFile")
    if processor is None:
        raise ValueError("GenerateFlowFile processor not found in FetchClips")
    return await _set_processor_state(client, processor, running=False)


async def _find_pg_by_name(client: httpx.AsyncClient, target_name: str) -> dict | None:
    """BFS from root for a single process group by name — a one-off lookup for PGs
    outside STREAMER_PG_NAMES (e.g. LiveStreamerAlert), not cached like that set."""
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
            if pg.get("component", {}).get("name") == target_name:
                return {"id": pg["id"], "version": pg.get("revision", {}).get("version", 0)}
            queue.append((pg["id"], depth + 1))
    return None


async def _find_processor(client: httpx.AsyncClient, pg_id: str, processor_name: str) -> dict | None:
    from services.nifi import _get
    data = await _get(client, f"/process-groups/{pg_id}/processors")
    for p in data.get("processors", []):
        if p.get("component", {}).get("name") == processor_name:
            return p
    return None


async def _set_processor_state(client: httpx.AsyncClient, processor: dict, running: bool) -> dict:
    """Processor-level run-status only — deliberately not a GET-then-PUT of the full
    processor entity, which would round-trip masked '********' sensitive properties
    back as literal values (the credential-wipe incident this pattern avoids)."""
    from services.nifi import _put
    body = {
        "revision": processor["revision"],
        "state": "RUNNING" if running else "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    return await _put(client, f"/processors/{processor['id']}/run-status", body)


async def run_live_streamer_alert_once(client: httpx.AsyncClient) -> dict:
    """Pulse PollTimer (GenerateFlowFile) on the LiveStreamerAlert PG for one poll
    cycle, then restore whatever state it was actually in before this ran.

    PollTimer is CRON_DRIVEN and RUNNING as its normal resting state (Steven's own
    live tuning, 2026-07-23) — not the STOPPED-by-default manual-pulse design this
    function originally assumed. Unconditionally forcing STOPPED at the end of
    every Telegram run was silently killing that recurring schedule. A separate
    ManualPollTrigger processor was tried to sidestep sharing a processor with the
    cron entirely, but starting the whole PG with it wired in broke the flow —
    reverted same day. Back to pulsing PollTimer directly; this function's actual
    fix is remembering RUNNING/STOPPED on entry and restoring that same state at
    the end instead of always stopping, so a Telegram run-once never leaves the
    cron schedule disabled.
    """
    pg = await _find_pg_by_name(client, LIVE_STREAMER_ALERT_PG_NAME)
    if not pg:
        raise ValueError(f"Process group '{LIVE_STREAMER_ALERT_PG_NAME}' not found")
    proc = await _find_processor(client, pg["id"], LIVE_STREAMER_ALERT_POLL_PROCESSOR)
    if not proc:
        raise ValueError(
            f"Processor '{LIVE_STREAMER_ALERT_POLL_PROCESSOR}' not found in '{LIVE_STREAMER_ALERT_PG_NAME}'"
        )

    was_running = proc["component"]["state"] == "RUNNING"
    if not was_running:
        await _set_processor_state(client, proc, running=True)
    await asyncio.sleep(5)
    # Re-fetch: starting mutates the processor's revision — reusing the stale one
    # from before the start would 409 the restore.
    proc = await _find_processor(client, pg["id"], LIVE_STREAMER_ALERT_POLL_PROCESSOR)
    if proc:
        await _set_processor_state(client, proc, running=was_running)
    return {"ok": True, "processor": LIVE_STREAMER_ALERT_POLL_PROCESSOR, "triggered": True, "restored_state": "RUNNING" if was_running else "STOPPED"}


# ── LiveStreamerAlert OAuth2 token refresh ──────────────────────────────────
#
# Incident 2026-08-04: GetTwitchLiveStatus 401'd on every request for an
# unknown number of days -- the watchlist only ever had Kick entries even
# though several roster Twitch streamers were live. Root cause: NiFi's
# StandardOauth2AccessTokenProvider only refetches a client_credentials token
# once its own tracked expiry elapses; if Twitch invalidates the token early
# (secret rotation, revocation, whatever) it keeps serving the same dead
# token to every poll forever with no detection, since a 401 from the API
# doesn't tell the provider "this token is bad, get a new one." Disabling
# then re-enabling the controller service forces a fresh token fetch -- same
# reset technique already used for LiveAlert MapCacheServer's dedup cache,
# applied here to the OAuth2 providers instead. Doing this preemptively on a
# schedule (see start_oauth_refresh_scheduler below) means a dead token gets
# replaced before anyone notices the watchlist looks wrong, instead of after.
LIVE_ALERT_OAUTH_PROVIDERS = (
    "Twitch OAuth2 (client_credentials)",
    "Kick OAuth2 (client_credentials)",
)

# Run well outside PollTimer's live-alert window (20-23,0-3 EDT, i.e.
# 00:00-08:00 UTC covering both EDT/EST) so the brief processor stop/start
# below never overlaps a real poll cycle.
_OAUTH_REFRESH_HOUR_UTC = 18


async def _find_controller_service(client: httpx.AsyncClient, pg_id: str, name: str) -> dict | None:
    """Controller services aren't scoped to the PG that uses them -- this walks up
    through ancestor groups too, same as NiFi's own UI dropdown does."""
    from services.nifi import _get
    data = await _get(client, f"/flow/process-groups/{pg_id}/controller-services")
    for entry in data.get("controllerServices", []):
        if entry.get("component", {}).get("name") == name:
            return entry
    return None


async def _set_controller_service_state(client: httpx.AsyncClient, cs: dict, state: str) -> dict:
    from services.nifi import _put
    comp = cs["component"]
    body = {"revision": cs["revision"], "component": {"id": comp["id"], "state": state}}
    return await _put(client, f"/controller-services/{comp['id']}", body)


async def refresh_live_alert_oauth_tokens(client: httpx.AsyncClient) -> dict:
    """Force both LiveStreamerAlert OAuth2 providers to fetch a fresh token.

    For each controller service: snapshot which of its referencing processors
    are running, disable the CS (NiFi auto-stops every processor using it),
    re-enable it (forces a fresh client_credentials fetch), then restart
    exactly the processors that were running -- the whole referencing set, not
    just the poller, so this never leaves a processor stopped that wasn't
    already stopped.
    """
    from services.nifi import _get

    pg = await _find_pg_by_name(client, LIVE_STREAMER_ALERT_PG_NAME)
    if not pg:
        raise ValueError(f"Process group '{LIVE_STREAMER_ALERT_PG_NAME}' not found")

    results: dict[str, str] = {}
    for cs_name in LIVE_ALERT_OAUTH_PROVIDERS:
        cs = await _find_controller_service(client, pg["id"], cs_name)
        if not cs:
            results[cs_name] = "not_found"
            continue

        # The disable auto-stops *every* processor referencing the CS, not just
        # the poller this refresh exists for -- GetKickChannelId shares the Kick
        # provider with GetKickLiveStatus and kept getting left stopped when
        # only the paired poller was restored (incident 2026-08-12). So the
        # restore set has to be the CS's own referencing components, not a
        # hardcoded processor name.
        full_cs = await _get(client, f"/controller-services/{cs['component']['id']}")
        was_running_ids = [
            ref["component"]["id"]
            for ref in full_cs["component"].get("referencingComponents", [])
            if ref["component"].get("referenceType") == "Processor"
            and ref["component"].get("state") == "RUNNING"
        ]

        await _set_controller_service_state(client, cs, "DISABLED")
        await asyncio.sleep(2)

        cs = await _find_controller_service(client, pg["id"], cs_name)
        await _set_controller_service_state(client, cs, "ENABLED")
        await asyncio.sleep(3)

        for proc_id in was_running_ids:
            proc = await _get(client, f"/processors/{proc_id}")
            await _set_processor_state(client, proc, running=True)

        results[cs_name] = "refreshed"

    return {"ok": True, "providers": results}


def _seconds_until_next_oauth_refresh() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=_OAUTH_REFRESH_HOUR_UTC, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


_oauth_refresh_task: asyncio.Task | None = None


async def start_oauth_refresh_scheduler(client: httpx.AsyncClient) -> None:
    """Daily background loop, one cycle/day at _OAUTH_REFRESH_HOUR_UTC. Intended to
    be run as a background task from the app's lifespan, same shape as
    chat_activity.start_aggregator()."""
    global _oauth_refresh_task
    _oauth_refresh_task = asyncio.current_task()
    try:
        while True:
            await asyncio.sleep(_seconds_until_next_oauth_refresh())
            try:
                result = await refresh_live_alert_oauth_tokens(client)
                logger.info("oauth refresh: %s", result)
            except Exception:
                logger.warning("oauth refresh cycle failed", exc_info=True)
    except asyncio.CancelledError:
        pass


async def stop_oauth_refresh_scheduler() -> None:
    global _oauth_refresh_task
    task = _oauth_refresh_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _oauth_refresh_task = None


# ── Trigger (ListenHTTP -> RouteOnAttribute) ────────────────────────────────
#
# StreamersApp has a single shared on-demand entry point: a ListenHTTP
# ("Trigger") feeds a RouteOnAttribute that branches on the X-Trigger-Request
# header to one of three TriggerInput ports (LiveStreamerAlert, FetchClips,
# PublishClipPeakTimeCron -- PublishClip's own PG has no TriggerInput, it's
# retired in favor of the peak-time cron one). One flowfile in, routed
# straight past each flow's own top-level scheduler (PollTimer's cron,
# FetchClips'/PublishClipPeakTimeCron's start/stop toggle) -- no PollTimer
# start/stop juggling like run_live_streamer_alert_once() above needs.
#
# request_name must match a RouteOnAttribute property name exactly; anything
# else silently lands in its auto-terminated 'unmatched' relationship with no
# error surfaced back here, so the allow-list below is load-bearing, not just
# validation.
TRIGGER_REQUESTS = ("LiveStreamerAlert", "FetchClips", "PublishClip", "PostWatchList")


async def trigger_flow(client: httpx.AsyncClient, request_name: str) -> dict:
    if request_name not in TRIGGER_REQUESTS:
        raise ValueError(f"Unknown trigger request '{request_name}', expected one of {TRIGGER_REQUESTS}")
    r = await client.post(
        settings.NIFI_TRIGGER_URL,
        headers={"X-Trigger-Request": request_name},
        timeout=30.0,
    )
    r.raise_for_status()
    return {"ok": True, "request": request_name, "status": r.status_code}


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
    login: str = "",
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

    lo, hi = _clip_duration_range(login, _DEFAULT_TWITCH_CLIP_RANGE)
    valid = [c for c in all_clips if lo <= c.get("duration", 0) <= hi]
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
    lo, hi = _clip_duration_range(slug, _DEFAULT_KICK_CLIP_RANGE)
    valid = [c for c in clips if lo <= c.get("duration", 0) <= hi]
    return sorted(valid, key=lambda c: c.get("view_count", 0), reverse=True)


async def is_streamer_live(client: httpx.AsyncClient, entry: str) -> bool:
    """True if 'entry' (bare Twitch login or 'kick:slug') is live right now.

    Used by TwitchChatListenerProcessor's !load live-check so the bot can reply
    instead of silently no-op-ing when the target isn't actually streaming.
    Any lookup failure (API error, unresolvable channel) returns False rather
    than raising, since the caller's fallback is just "not live"."""
    platform, name = _parse_watch_entry(entry)
    try:
        if platform == "kick":
            token = await _kick_token_refresh(client)
            broadcaster_id = await _get_kick_broadcaster_id(client, token, name)
            if broadcaster_id is None:
                return False
            r = await client.get(
                f"{_KICK_API_BASE}/users/livestreams",
                params={"user_id": broadcaster_id},
                headers=_kick_headers(token),
                timeout=10.0,
            )
        else:
            token = await _twitch_token_refresh(client)
            r = await client.get(
                "https://api.twitch.tv/helix/streams",
                params={"user_login": name},
                headers=_twitch_headers(token),
                timeout=10.0,
            )
        if r.status_code != 200:
            return False
        return bool(r.json().get("data"))
    except Exception:
        return False


async def discover_top_unfollowed(client: httpx.AsyncClient, limit: int = 5) -> list[str]:
    """Top live Twitch streams by viewer count (Helix's default sort when
    called with no user_login filter) that aren't already in get_roster() --
    for TopStreamerJoiner's chat-presence-expansion bot. Deliberately doesn't
    touch the watchlist/roster itself; the caller (TopStreamerJoiner) just
    joins their chat to say hi, it doesn't feed the clip-fetch pipeline the
    way add_to_watchlist() does."""
    token = await _twitch_token_refresh(client)
    r = await client.get(
        "https://api.twitch.tv/helix/streams",
        params={"first": "100"},
        headers=_twitch_headers(token),
        timeout=10.0,
    )
    r.raise_for_status()
    known = {e.lower() for e in get_roster()}
    candidates: list[str] = []
    for stream in r.json().get("data", []):
        login = (stream.get("user_login") or "").lower()
        if login and login not in known and login not in candidates:
            candidates.append(login)
        if len(candidates) >= limit:
            break
    return candidates


_ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
_OVERLAY_FONT = _ASSETS_DIR / "fonts" / "DejaVuSans-Bold.ttf"
_OVERLAY_LOGOS = {
    "kick": _ASSETS_DIR / "logos" / "kick.png",
    "twitch": _ASSETS_DIR / "logos" / "twitch.png",
}
_OVERLAY_DOMAINS = {"kick": "KICK.COM", "twitch": "TWITCH.TV"}
# Kick's cropped wordmark reads smaller than Twitch's at the same pixel height, so it gets a bigger ratio.
_OVERLAY_LOGO_HEIGHT_RATIO = {"kick": 0.13, "twitch": 0.16}

# Tiny tuna mascot, placed right before the PLATFORM.COM/HANDLE label. Sized off
# font_size (not bar_h) so it scales with the text rather than the bar. Ratios
# tuned against a mockup on DesktopShare issue #87 (Steven: "perfect, ship that").
_TUNA_ICON = _ASSETS_DIR / "icons" / "tuna.png"
_TUNA_ICON_HEIGHT_RATIO = 1.7
_TUNA_ICON_GAP_RATIO = 0.4
_TUNA_ICON_Y_NUDGE_RATIO = 0.35

# Base font size ratio (fraction of clip height) — the size used for short
# streamer names. Long names shrink down from this, never grow past it.
_OVERLAY_FONT_SIZE_RATIO = 0.07
# Never shrink the label past this fraction of clip height, however long the
# name — it'd stop reading as a link at all.
_OVERLAY_MIN_FONT_SIZE_RATIO = 0.04

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


def _probe_video_duration(path: Path) -> float | None:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "csv=p=0", str(path),
            ],
            capture_output=True, text=True, timeout=15,
        )
        return float(result.stdout.strip())
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
    bar_h = round(height * 0.19 / 2) * 2  # keep even — libx264 needs even dimensions
    new_height = height + bar_h
    logo_h = round(height * _OVERLAY_LOGO_HEIGHT_RATIO.get(source, 0.16))
    left_pad = round(width * 0.0146)
    right_pad = round(width * 0.0208)
    label = f"{_OVERLAY_DOMAINS.get(source, source.upper())}/{streamer.upper()}"
    tmp = dest.with_suffix(".overlay.mp4")

    # Logo's rendered width, needed to know how much room is left for the
    # label — scale=-1 in the filter graph derives it from logo_h the same way.
    with Image.open(logo_path) as logo_img:
        logo_w = round(logo_h * logo_img.width / logo_img.height)

    # Shrink the font (down to a floor) if the label would otherwise run into
    # the logo — long streamer names at the fixed base size overflowed past
    # the available width (DesktopShare issue #135). Both the label text and
    # the tuna icon scale ~linearly with font_size, so measuring once at the
    # base size and scaling by the overflow ratio gets the fit right without
    # iterating.
    max_font_size = round(height * _OVERLAY_FONT_SIZE_RATIO)
    min_font_size = round(height * _OVERLAY_MIN_FONT_SIZE_RATIO)
    inter_gap = round(width * 0.02)
    available_w = width - left_pad - logo_w - inter_gap - right_pad

    label_w = round(ImageFont.truetype(str(_OVERLAY_FONT), max_font_size).getlength(label))
    content_w = label_w
    tuna_available = _TUNA_ICON.exists()
    if tuna_available:
        with Image.open(_TUNA_ICON) as tuna_img:
            tuna_aspect = tuna_img.width / tuna_img.height
        icon_w_at_max = round(max_font_size * _TUNA_ICON_HEIGHT_RATIO * tuna_aspect)
        icon_gap_at_max = round(max_font_size * _TUNA_ICON_GAP_RATIO)
        content_w += icon_w_at_max + icon_gap_at_max

    font_size = max_font_size
    if content_w > available_w > 0:
        font_size = max(min_font_size, round(max_font_size * available_w / content_w))
        label_w = round(ImageFont.truetype(str(_OVERLAY_FONT), font_size).getlength(label))

    # Tuna mascot goes right before the label, sized/gapped off font_size so it
    # tracks the label's actual rendered width (varies with streamer name
    # length) instead of a fixed offset that would drift for short/long names.
    inputs = ["-i", str(dest), "-i", str(logo_path)]
    tuna_stage = ""
    text_input = "v1"
    if tuna_available:
        icon_h = round(font_size * _TUNA_ICON_HEIGHT_RATIO)
        icon_gap = round(font_size * _TUNA_ICON_GAP_RATIO)
        icon_y = round((bar_h - icon_h) / 2) + round(font_size * _TUNA_ICON_Y_NUDGE_RATIO)
        icon_left = width - right_pad - label_w - icon_gap
        inputs += ["-i", str(_TUNA_ICON)]
        tuna_stage = f"[2:v]scale=-1:{icon_h}[tuna];[v1][tuna]overlay=x={icon_left}-overlay_w:y={icon_y}[v2];"
        text_input = "v2"

    filter_complex = (
        f"[0:v]pad=width={width}:height={new_height}:x=0:y={bar_h}:color=black[padded];"
        f"[1:v]scale=-1:{logo_h}[logo];"
        f"[padded][logo]overlay=x={left_pad}:y=({bar_h}-overlay_h)/2[v1];"
        f"{tuna_stage}"
        f"[{text_input}]drawtext=fontfile={_OVERLAY_FONT}:text='{label}':fontcolor=white:"
        f"fontsize={font_size}:x=w-tw-{right_pad}:y={bar_h}-th-{right_pad}[vout]"
    )
    try:
        with _overlay_lock():
            result = subprocess.run(
                [
                    "ffmpeg", "-y", "-threads", "1", *inputs,
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
    if not settings.GLITCH_INTRO_ENABLED:
        return False
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
            strobe_variants = variant_pool[:8]

            # Longer, more dramatic front-loaded distortion (watch-rate hook —
            # the strobe needs to read clearly in the first couple of seconds).
            hold_dur = round(random.uniform(1.0, 1.8), 2)
            fade_dur = round(random.uniform(1.8, 2.5), 2)

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

            # Snap-back: unwind the same distortion in reverse (strobe -> crisp) instead
            # of hard-cutting straight into the real footage below.
            fadeout = tmp / "fadeout.mp4"
            if not run(["ffmpeg", "-y", "-threads", "1", "-i", str(strobe_seq), "-i", str(crisp_clip),
                        "-filter_complex",
                        f"[0:v][1:v]xfade=transition=fade:duration={fade_dur}:offset=0[v]",
                        "-map", "[v]", "-an", "-c:v", "libx264", "-preset", "veryfast", "-threads", "1",
                        "-crf", "20", "-bf", "0", "-x264opts", "threads=1:sliced-threads=0:scenecut=0",
                        str(fadeout)]):
                return False

            region_list = tmp / "region_list.txt"
            region_list.write_text(f"file '{hold_mp4}'\nfile '{fadeflash}'\nfile '{fadeout}'")
            region_mp4 = tmp / "region.mp4"
            if not run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(region_list),
                        "-c", "copy", str(region_mp4)]):
                return False

            if bar_h > 0:
                total_dur = hold_dur + fade_dur * 2
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
    _atomic_write_json(path, sorted(ids))


def _skipped_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".skipped.json"


def _published_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".published.json"


def _pending_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".pending_publish.json"


def _published_history_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".published_history.json"


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
    _atomic_write_json(_fetch_mode_path(), data)
    return data


# Catalog: bare login (lowercase) → X handle (no @ prefix).
# Source of truth: DesktopShare/streamers.md — keep both in sync when adding streamers.
_STREAMER_CATALOG: dict[str, str] = {
    # Twitch
    "xqc":            "xQc",
    "stableronaldo":  "StableRonaldo",
    "jynxzi":         "jynxzi",
    "extraemily":     "ExtraEmilyy",
    "theburntpeanut": "theburntpeanut",
    "jasontheween":   "jasontheween",
    "lacy":           "LacyHimself",
    "kaicenat":       "KaiCenat",
    # Kick
    "roshtein":       "roshtein",
    "ac7ionman":      "Ac7ionMann",
    "adinross":       "adinross",
    "n3on":           "n3ononyt",
    "clavicular":     "Clavicular0",
    "bbjess":         "bbjess",
    "whiz":           "crashoverride",
    "trainwreckstv":  "trainwreckstv",
    "rampagejackson": "rampagejackson",
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
    _atomic_write_json(_pending_path(), pending)


def mark_skipped(clip_id: str) -> None:
    ids = _load_id_set(_skipped_path())
    ids.add(clip_id)
    _save_id_set(_skipped_path(), ids)


def mark_published(
    clip_id: str, title: str = "", source: str = "", streamer: str = "",
    url: str = "", thumbnail_url: str = "", x_handle: str = "",
    tweet_id: str = "", tweet_url: str = "",
) -> None:
    ids = _load_id_set(_published_path())
    ids.add(clip_id)
    _save_id_set(_published_path(), ids)

    with _pending_lock():
        p = _published_history_path()
        history = []
        if p.exists():
            try:
                history = json.loads(p.read_text())
            except Exception:
                history = []
        history.append({
            "clip_id": clip_id, "title": title, "source": source, "streamer": streamer,
            "url": url, "thumbnail_url": thumbnail_url, "x_handle": x_handle,
            "tweet_id": tweet_id, "tweet_url": tweet_url,
            "published_at": datetime.now(timezone.utc).isoformat(),
        })
        _atomic_write_json(p, history[-500:])


def get_published_history(limit: int = 60) -> list[dict]:
    """Most-recently-published clips first, for the Posted Clips tile gallery."""
    p = _published_history_path()
    if not p.exists():
        return []
    try:
        history = json.loads(p.read_text())
    except Exception:
        return []
    return list(reversed(history))[:limit]


def _patch_missing_metadata(entry: dict, meta: dict) -> bool:
    changed = False
    for field in ("title", "source", "streamer", "url", "thumbnail_url", "view_count",
                  "duration", "created_at"):
        if not entry.get(field) and meta.get(field):
            entry[field] = meta[field]
            changed = True
    if not entry.get("x_handle") and entry.get("streamer"):
        xh = get_x_handle(entry["streamer"])
        if xh:
            entry["x_handle"] = xh
            changed = True
    return changed


async def backfill_metadata() -> dict:
    """One-time repair for pending/published entries created before source/streamer/
    url/thumbnail_url/x_handle were added to approve_clip()/mark_published(). Full
    scan of processed_clips (every message still has the original fetch metadata,
    since Kafka topics aren't mutated when a clip is later approved/published) to
    build a clip_id -> metadata lookup, then fills in only the missing fields on
    existing pending/published-history entries. Safe to re-run — a no-op once
    every entry already has its fields.
    """
    topic = settings.PROCESSED_CLIPS_TOPIC
    by_id: dict[str, dict] = {}
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        request_timeout_ms=15000,
    )
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        begin = (await consumer.beginning_offsets([tp]))[tp]
        end = (await consumer.end_offsets([tp]))[tp]
        consumer.seek(tp, begin)
        fetched = 0
        while fetched < (end - begin):
            batch = await consumer.getmany(tp, timeout_ms=5000, max_records=500)
            msgs = batch.get(tp, [])
            if not msgs:
                break
            for msg in msgs:
                try:
                    rec = json.loads(msg.value.decode("utf-8"))
                    cid = rec.get("clip_id", "")
                    if cid:
                        by_id[cid] = rec
                except Exception:
                    pass
            fetched += len(msgs)
    finally:
        await consumer.stop()

    with _pending_lock():
        pending = _load_pending()
        pending_patched = sum(
            1 for entry in pending
            if entry.get("clip_id") in by_id and _patch_missing_metadata(entry, by_id[entry["clip_id"]])
        )
        _save_pending(pending)

        hist_path = _published_history_path()
        history = []
        if hist_path.exists():
            try:
                history = json.loads(hist_path.read_text())
            except Exception:
                history = []
        history_patched = sum(
            1 for entry in history
            if entry.get("clip_id") in by_id and _patch_missing_metadata(entry, by_id[entry["clip_id"]])
        )
        _atomic_write_json(hist_path, history)

    return {
        "indexed": len(by_id),
        "pending_total": len(pending), "pending_patched": pending_patched,
        "published_total": len(history), "published_patched": history_patched,
    }


def get_skipped() -> set[str]:
    return _load_id_set(_skipped_path())


def get_published() -> set[str]:
    return _load_id_set(_published_path())


def approve_clip(
    clip_id: str, clip_path: str, tweet_text: str, title: str = "",
    source: str = "", streamer: str = "", url: str = "",
    thumbnail_url: str = "", x_handle: str = "", view_count: int = 0,
    duration: float = 0, created_at: str = "",
) -> dict:
    """Queue a clip for X publishing. Returns immediately — NiFi drains the queue.

    Locked read-modify-write: without this, two near-simultaneous approvals (or an
    approval racing publish_next) can each read the same pending list and overwrite
    each other's append, silently dropping an approved clip from the queue.

    Carries the display metadata (source/streamer/url/thumbnail/x_handle/view_count/
    duration/created_at) through so the Pending Publish panel can render a full card
    instead of just clip_id + text.
    """
    with _pending_lock():
        pending = _load_pending()
        if not any(p["clip_id"] == clip_id for p in pending):
            pending.append({
                "clip_id": clip_id, "clip_path": clip_path, "tweet_text": tweet_text, "title": title,
                "source": source, "streamer": streamer, "url": url,
                "thumbnail_url": thumbnail_url, "x_handle": x_handle, "view_count": view_count,
                "duration": duration, "created_at": created_at,
            })
            _save_pending(pending)
    return {"queued": True, "clip_id": clip_id, "position": len(pending)}


async def publish_pending(clip_id: str) -> dict:
    """Publish one specific pending clip right now, regardless of its queue position.

    Same pop-and-save-under-lock shape as publish_next(), except it removes the
    matching clip_id from wherever it sits in the list instead of always index 0.
    The pending list is a flat JSON file, not Kafka — removing one entry out of
    order doesn't touch Kafka offsets or the relative order of the remaining clips.
    """
    with _pending_lock():
        pending = _load_pending()
        clip = next((p for p in pending if p["clip_id"] == clip_id), None)
        if clip is None:
            return {"published": False, "reason": "not in pending queue"}
        remaining = [p for p in pending if p["clip_id"] != clip_id]
        _save_pending(remaining)
    try:
        result = await publish_clip(
            clip["clip_path"], clip["tweet_text"], clip["clip_id"], clip.get("title", ""),
            clip.get("source", ""), clip.get("streamer", ""), clip.get("url", ""),
            clip.get("thumbnail_url", ""), clip.get("x_handle", ""),
        )
    except Exception:
        with _pending_lock():
            _save_pending([clip] + _load_pending())
        raise
    return {**result, "queue_remaining": len(remaining)}


async def publish_next() -> dict:
    """Pop the first pending clip and publish it to X. Called by NiFi timer every 2 min.

    The pop-and-save happens under the same lock as approve_clip/cancel_pending so the
    queue stays strict FIFO even if NiFi fires overlapping calls (e.g. a slow upload
    causing the next GenerateFlowFile tick to overlap the previous InvokeHTTP). The
    slow network publish itself runs outside the lock so it doesn't block approvals.

    If the publish attempt raises (e.g. X rejects the clip), the clip is put back at
    the front of the queue instead of being dropped — a permanently-unpublishable
    clip (like an oversized video) can then be seen and cancelled via /pending
    instead of silently vanishing.
    """
    with _pending_lock():
        pending = _load_pending()
        if not pending:
            return {"published": False, "reason": "queue empty"}
        clip = pending[0]
        _save_pending(pending[1:])
    try:
        result = await publish_clip(
            clip["clip_path"], clip["tweet_text"], clip["clip_id"], clip.get("title", ""),
            clip.get("source", ""), clip.get("streamer", ""), clip.get("url", ""),
            clip.get("thumbnail_url", ""), clip.get("x_handle", ""),
        )
    except Exception:
        with _pending_lock():
            _save_pending([clip] + _load_pending())
        raise
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


# Streamers processed per FetchClips run, rotating through the watch list one
# (or a few) at a time instead of all of them every run. Each run's per-clip
# ffmpeg overlay burn is serialized (see _overlay_lock), and on the pod's 1-CPU
# limit that was pushing whole-watch-list runs well past the 15 min NiFi cron
# interval, backing up the FetchClips->InvokeHTTP queue. Keeping batches small
# bounds each run's wall time; the cron interval was shortened to compensate
# so each streamer is still visited about as often overall.
_FETCH_BATCH_SIZE = 1


def _fetch_rotation_path() -> Path:
    return Path(settings.CLIP_STORAGE_PATH) / ".fetch_rotation.json"


def _next_fetch_batch(logins: list[str], batch_size: int) -> list[str]:
    """Return the next `batch_size` logins to fetch this run and advance the
    persisted rotation index, wrapping around the watch list.

    Index is taken mod the *current* watch list length so a watch list resize
    (add/remove/rotate) between runs can't put it out of range.
    """
    p = _fetch_rotation_path()
    index = 0
    if p.exists():
        try:
            index = json.loads(p.read_text()).get("index", 0)
        except Exception:
            index = 0
    index %= len(logins)
    batch = [logins[(index + i) % len(logins)] for i in range(min(batch_size, len(logins)))]
    _atomic_write_json(p, {"index": (index + batch_size) % len(logins)})
    return batch


async def fetch_clips() -> dict:
    """Poll Twitch and Kick for clips from a rotating slice of the watch list,
    download to PVC, publish to new_clips.

    Watch list entries are 'login' (Twitch) or 'kick:login' (Kick). Only
    _FETCH_BATCH_SIZE streamers are fetched per run (see _next_fetch_batch) —
    the clip cap below still scales off the *full* watch list so per-streamer
    volume stays the same as before, just spread across more, smaller runs.
    """
    logins = get_watchlist()
    if not logins:
        return {"fetched": 0, "clips": [], "error": "Watch list is empty"}

    clip_cap = _clips_per_streamer_cap(len(logins))
    logins = _next_fetch_batch(logins, _FETCH_BATCH_SIZE)

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
        _atomic_write_json(seen_file, list(seen))

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
    clips = await _get_clips(client, token, broadcaster_id, since, login=login, top_mode=top_mode)
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
            # Trim before burning the overlay/intro — see matching comment in
            # _fetch_kick_clips.
            _, _, trim_err = await asyncio.get_event_loop().run_in_executor(
                None, _trim_if_oversized, dest
            )
            if trim_err:
                errors.append(f"Twitch: trim failed for {clip_id}: {trim_err}")
            bar_h = await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, l=login: _burn_platform_overlay(d, "twitch", l)
            )
            await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, b=bar_h: _burn_glitch_intro(d, b)
            )
        # Measure the file we actually produced rather than trusting the platform's
        # self-reported duration — the API value predates our overlay/intro burn and
        # can drift arbitrarily far from reality (e.g. the source download itself
        # coming back bloated), silently sailing through the validity filter above.
        real_duration = await asyncio.get_event_loop().run_in_executor(
            None, _probe_video_duration, dest
        )
        result.append({
            "clip_id": clip_id,
            "source": "twitch",
            "streamer": login,
            "broadcaster_id": broadcaster_id,
            "title": html.unescape(clip.get("title", "")),
            "url": clip.get("url", ""),
            "thumbnail_url": clip.get("thumbnail_url", ""),
            "duration": real_duration if real_duration is not None else clip.get("duration", 0),
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
            # Trim before burning the overlay/intro, not just at publish time —
            # a per-streamer window (see _CLIP_DURATION_OVERRIDES) can admit
            # clips well past MAX_TWEET_VIDEO_DURATION, and trimming here means
            # the reviewer previews the same cut that will actually post instead
            # of getting silently re-cut later.
            _, _, trim_err = await asyncio.get_event_loop().run_in_executor(
                None, _trim_if_oversized, dest
            )
            if trim_err:
                errors.append(f"Kick: trim failed for {clip_id}: {trim_err}")
            bar_h = await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, l=login: _burn_platform_overlay(d, "kick", l)
            )
            await asyncio.get_event_loop().run_in_executor(
                None, lambda d=dest, b=bar_h: _burn_glitch_intro(d, b)
            )
        # Measure the file we actually produced rather than trusting the platform's
        # self-reported duration — see matching comment in _fetch_twitch_clips.
        real_duration = await asyncio.get_event_loop().run_in_executor(
            None, _probe_video_duration, dest
        )
        result.append({
            "clip_id": clip_id,
            "source": "kick",
            "streamer": login,
            "title": html.unescape(clip.get("title", "")),
            "url": clip.get("clip_url", ""),
            "thumbnail_url": clip.get("thumbnail_url", ""),
            "duration": real_duration if real_duration is not None else clip.get("duration", 0),
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


_URL_RE = re.compile(
    r'(?:https?://|www\.)\S+'                                  # http(s)://... or www....
    r'|\b(?:[\w-]+\.)+(?:com|net|org|co|tv|gg|io|me)\b/?\S*',  # bare domain(/path), e.g. pic.twitter.com/xxxx
    flags=re.IGNORECASE,
)

# A large pool of few-shot examples, only a random subset of which is shown per
# call (see process_clip) — showing the SAME few-shot examples on every call
# anchors the model onto their exact sentence shapes across independent requests,
# which reads as repetitive/templated even with frequency/presence penalty (those
# only affect variety *within* one completion). Placeholder names throughout —
# never reused verbatim, purely illustrative of tone/shape.
_CAPTION_EXAMPLES = [
    "nobody does it like riven, the rest of this lobby should just log off 💀",
    "kai really said 'trust me' right before eating that grenade 😭",
    "'i got this' — kai, three seconds before absolutely not getting this 🤡",
    "the way sable just no-scoped that and immediately started crying, we are not the same 🔥",
    "not jay tripping over their own feet and still calling it a strat 💀",
    "ace out here cooking everyone and acting like it's normal, humble yourself 😤",
    "remy said 'watch this' and then immediately did not watch this 🤡",
    "the confidence on nova walking into that ambush like nothing could go wrong, incredible 😭",
    "blaze just hit a shot the pros can't hit and is somehow still complaining about ping 🔥",
    "'trust the process' — jay, right before the process fell off a cliff 💀",
    "chat, remy just talked their way out of a 1v1 they were clearly losing, absolute menace 😤",
    "not ace calling that a clean rotation, my grandma rotates better than that 💀",
    "the way nova celebrated before the round even ended, some people never learn 😭",
    "blaze woke up and chose violence today, everyone else can just take the L 🔥",
    "'i never miss' — sable, immediately missing 🤡",
    "jay out here making decisions like it's their first day, chat is not okay 😤",
    "the disrespect from ace not even looking at the kill feed, that's how confident they are 🔥",
    "not remy blaming the controller for that one, we all saw it live 💀",
    "nova said 'easy' and then it was extremely not easy, love that for us 😭",
    "chat, blaze just did something the algorithm is going to be showing people for weeks 🔥",
    "'no way that worked' — even ace sounds surprised at their own play 🤡",
    "the way jay just walked past three enemies like they were furniture, unreal 💀",
    "sable calling that a masterclass is sending me, my brother in christ that was chaos 😭",
    "not nova rage quitting the callout mid-clip, iconic behavior honestly 🤡",
    "ace really thinks this is normal gameplay, respectfully it is not 😤",
    "'watch me clutch this' — remy, three deaths later, still talking 💀",
]

# A transcript this thin gives the LLM nothing real to react to — it was
# inventing unrelated/mean filler just to produce a caption-length response.
# At or under this many words, quote what was actually said instead.
_SHORT_TRANSCRIPT_WORDS = 6

_CAPTION_OPENER_STYLES = [
    "a mock-shocked aside (e.g. start with 'the way {name}...' or 'not {name}...')",
    "a direct jab/roast at the streamer over what just happened",
    "a cocky bragging comparison putting {name} above everyone else",
    "a quote lead-in — open by quoting or paraphrasing the funniest/most confident line from the transcript, then react",
    "a callout to chat/viewers about what {name} just pulled, framed as disbelief or hype",
]

# First-person "Tuna is watching" framing line prepended ahead of the reaction
# body (see _CAPTION_OPENER_STYLES above for that part) — always emoji-led per
# Steven's explicit ask, one random pick per post so it doesn't read as a
# templated bot. Modeled directly on his own real posted examples.
_WATCHING_INTROS = [
    "🎞️ Hot off the press, clipped:",
    "📺 I just joined {name}'s live stream, watching now, here's a clip you should watch:",
    "🔴 Watching {name} live right now, come join us! If you can't, you just missed this:",
    "🎬 Caught this live from {name}, had to share:",
    "👀 Currently watching {name} — check this out:",
    "⚡ Live with {name} right now, here's what just happened:",
]

# Same first-person framing as _WATCHING_INTROS, but honest about the clip no
# longer being live — used instead whenever the post is actually going out well
# after capture, or the streamer isn't live anymore. Which pool gets used is
# decided at actual publish time by _choose_intro, not baked in here at
# process_clip() time — a clip can sit in the pending queue for hours.
_MISSED_INTROS = [
    "📼 Missed {name} live? Here's what happened:",
    "🌙 Catch up on what {name} did on stream:",
    "⏪ Watch what you missed from {name}:",
    "🎬 In case you missed {name} live, here's the clip:",
    "👀 Something from {name}'s stream you might've missed:",
    "🕐 {name} went live earlier — here's the highlight:",
    "📌 From {name}'s last stream, in case you weren't there:",
]

# Straight random.choice() over a 5-7 item pool repeats noticeably within a
# handful of posts at this account's actual cadence (a post every ~10-30 min) —
# confirmed 2026-07-31 when the last several posts in a row all landed on one
# of two _MISSED_INTROS lines. Tracks the last few template strings actually
# used (pre-.format(), so different streamer names on the same template still
# count as a repeat) and excludes them from the next pick. In-memory only
# (not persisted) — worst case a redeploy resets it and one post might repeat,
# which is an acceptable trade for not adding another JSON state file to the
# non-atomic-write pile (see streamers.py module docstring).
_RECENT_INTROS_MAX = 3
_recent_intros: list[str] = []


def _choose_intro(streamer_name: str, is_live: bool) -> str:
    """Picks the "live now" framing whenever the streamer is confirmed live at
    actual publish time — that live check (is_streamer_live, called right
    before this) is already ground truth, checked in real time, so it doesn't
    matter how old the underlying clip capture is: if they're live right now,
    "come join us" is true. Only falls back to "missed it" framing when the
    live check says they're not live (or the check itself failed/errored —
    is_streamer_live returns False on any error, which is the safe default
    here too). See publish_clip() for where this is actually called."""
    pool = _WATCHING_INTROS if is_live else _MISSED_INTROS
    candidates = [t for t in pool if t not in _recent_intros] or pool
    template = random.choice(candidates)
    _recent_intros.append(template)
    del _recent_intros[:-_RECENT_INTROS_MAX]
    return template.format(name=streamer_name)


# A run of the same short substring repeated many times in a row — the
# degenerate "zerszerszerszers..." decoding failure mode small models can fall into.
_REPETITION_RE = re.compile(r'(.{2,20}?)\1{4,}', flags=re.DOTALL)


def _has_degenerate_repetition(text: str) -> bool:
    """True if text contains a runaway repeated substring (decoding failure).

    Only flags units with real character variety (e.g. 'zers') so normal expressive
    repetition like 'no no no no no' or 'AAAAAAAH' — same 1-2 letters over and over —
    doesn't get mistaken for a decoding failure.
    """
    for m in _REPETITION_RE.finditer(text):
        distinct = {c for c in m.group(1).lower() if c.isalnum()}
        if len(distinct) >= 3:
            return True
    return False


# Prompt-only gender rule (2026-07-16, tightened 2026-07-20) still leaves a
# measured residual violation rate on ambiguous/thin-transcript clips. This is
# the code-level safety net for that residual — every streamer, not just
# lacyhimself (widened 2026-07-23, supersedes the Lacy-only scope from
# 2026-07-22: we don't know any streamer's gender, so the guard applies
# uniformly). Word-boundary match so it doesn't false-positive on substrings
# like "hershey" or "there".
_GENDERED_PRONOUN_RE = re.compile(
    r'\b(?:she|her|hers|herself|he|him|his|himself)\b', flags=re.IGNORECASE
)


def _has_gendered_pronoun(text: str) -> bool:
    """True if text contains a whole-word he/him/his/she/her/hers pronoun."""
    return bool(_GENDERED_PRONOUN_RE.search(text))


def _clean_caption(text: str) -> str:
    """Strip model formatting artifacts from vLLM caption output."""
    text = html.unescape(text).strip()
    # Only the first paragraph is the caption; the rest is model commentary
    text = text.split("\n\n")[0].strip()
    # Strip leading label: **Word(s):** or "Word(s):"
    text = re.sub(r'^\*{0,2}[\w][\w ]*\*{0,2}:\s*', '', text)
    # Strip a leading placeholder token the model sometimes emits instead of
    # real content, e.g. "_reaction_ " or "*Reaction* " with no colon at all
    text = re.sub(r'^[_*]{1,2}[\w ]{1,20}[_*]{1,2}\s+', '', text)
    # Strip surrounding double-quotes that the model adds around the answer
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    # Strip any raw HTML tags that slipped through from a title/transcript
    text = re.sub(r'<[^>]+>', '', text)
    # Strip all hashtags — platform/handle suffix is added by _build_tweet
    text = re.sub(r'\s*#\w+', '', text)
    # Strip any @mentions the model invented — the only @handle in a posted
    # tweet must be the real streamer handle _build_tweet appends afterward
    text = re.sub(r'\s*@\w+', '', text)
    # Strip any URL-like token — X rejects the whole tweet with a 400 if a
    # hallucinated "link" (e.g. a fake pic.twitter.com/xxxx) isn't a real URL
    text = _URL_RE.sub('', text)
    text = re.sub(r'\s{2,}', ' ', text)
    # Cap emojis — if model spammed more than 3 total, strip them all
    emoji_matches = _EMOJI_RE.findall(text)
    if sum(len(m) for m in emoji_matches) > 3:
        text = _EMOJI_RE.sub("", text)
    return text.strip()


# ── Quoted-post fallback (#175) ───────────────────────────────────────────────
# When the model can't actually tell what the streamer is talking about, forcing
# the cocky-gaming-chat persona anyway makes it fabricate (2026-08-16, bbjess:
# an IRL relationship clip became "just flipped a 1v3" plus a fully invented
# quote). In that case the post is a verbatim quote of the streamer instead of
# an AI reaction — same format the ≤_SHORT_TRANSCRIPT_WORDS path already uses.

# Straight/curly double-quoted spans, and single-quoted spans bounded by
# whitespace (so apostrophes inside words don't open a span).
_QUOTE_SPAN_RES = [
    re.compile(r'["“]([^"“”]{3,200})["”]'),
    re.compile(r"(?:^|\s)'([^']{3,200})'(?=[\s,.!?…]|$)"),
]

_QUOTE_MAX_WORDS = 24


def _norm_quote_text(text: str) -> str:
    """Lowercase, strip everything but letters/digits/spaces, collapse runs —
    so a verbatim-copied span still matches the transcript across punctuation,
    casing, and apostrophe differences."""
    text = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _has_fabricated_quote(caption: str, transcript: str) -> bool:
    """True if the caption quote-marks a span of 3+ words that doesn't appear
    verbatim in the transcript — the model invented a quote."""
    haystack = _norm_quote_text(transcript)
    for span_re in _QUOTE_SPAN_RES:
        for span in span_re.findall(caption):
            norm = _norm_quote_text(span)
            if len(norm.split()) >= 3 and norm not in haystack:
                return True
    return False


def _quoted_caption(quote: str, streamer_name: str) -> str:
    return f'"{quote.strip()}" — {streamer_name}'


# Generic words the topic sentence can't earn grounding credit for — function
# words plus the framing vocabulary the model uses whether or not it understood
# anything ("the streamer is talking about...").
_TOPIC_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "of", "to", "in", "on", "at", "is",
    "are", "was", "were", "be", "being", "been", "about", "with", "for", "from",
    "their", "they", "them", "this", "that", "these", "those", "it", "its",
    "his", "her", "hers", "he", "she", "him", "himself", "herself", "someone",
    "something", "thing", "things", "person", "people", "viewers", "chat",
    "streamer", "stream", "streaming", "live", "clip", "talking", "talks",
    "discussing", "discusses", "describing", "saying", "says", "doing", "does",
    "possibly", "related", "recent", "some", "kind", "sort", "how", "what",
    "who", "when", "where", "why", "while", "during", "very", "really",
}


def _topic_grounded(topic: str, transcript: str, streamer_name: str) -> bool:
    """True if at least one content word of the claimed topic actually appears
    in the transcript (normalized; 4-char prefix match covers inflections like
    betting/bets). Asked what a pure-filler transcript is about, the model
    confidently invents a topic with zero word overlap — self-rated confidence
    doesn't catch that (measured 2026-08-16 on Qwen2.5-3B), this does."""
    stop = _TOPIC_STOPWORDS | set(_norm_quote_text(streamer_name).split())
    twords = set(_norm_quote_text(transcript).split())
    tprefix = {w[:4] for w in twords if len(w) >= 4}
    for w in _norm_quote_text(topic).split():
        if len(w) < 3 or w in stop:
            continue
        if w in twords or (len(w) >= 4 and w[:4] in tprefix):
            return True
    return False


async def _comprehend_topic(client: httpx.AsyncClient, transcript: str, streamer_name: str) -> str:
    """One short sentence of what the streamer is talking about, or "" when the
    AI can't actually figure it out. The model always answers a TOPIC (asking it
    to refuse with an UNCLEAR token made it refuse on nearly every real messy
    transcript — measured 2026-08-16), so "can't figure it out" is detected
    instead by _topic_grounded's word-overlap check, a LOW self-rating, a
    malformed reply, or any error (an unreachable vLLM quotes rather than
    drops the clip)."""
    try:
        r = await client.post(
            f"{settings.VLLM_URL}/v1/chat/completions",
            json={
                "model": settings.VLLM_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": "You analyze live-stream clip transcripts. Follow the output format exactly.",
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Read this transcript from {streamer_name}'s live stream. It is raw "
                            f"speech-to-text of casual talk — broken grammar, filler, and repetition "
                            f"are normal, read through them.\n"
                            f"Answer in exactly two lines:\n"
                            f"TOPIC: <one short sentence — what {streamer_name} is talking about or doing>\n"
                            f"CONFIDENCE: <HIGH, MEDIUM, or LOW — how sure you are that your TOPIC "
                            f"line is really what the clip is about>\n\n"
                            f"Transcript: {transcript[:600]}"
                        ),
                    },
                ],
                "max_tokens": 80,
                "temperature": 0.2,
            },
        )
        if r.status_code != 200:
            return ""
        ans = r.json()["choices"][0]["message"]["content"].strip()
        topic, confidence = "", ""
        for line in ans.split("\n"):
            line = line.strip()
            if line.upper().startswith("TOPIC:"):
                topic = line[len("TOPIC:"):].strip()
            elif line.upper().startswith("CONFIDENCE:"):
                confidence = line[len("CONFIDENCE:"):].strip().upper()
        if not topic or "LOW" in confidence:
            return ""
        if not _topic_grounded(topic, transcript, streamer_name):
            return ""
        return topic[:200]
    except Exception:
        return ""


async def _extract_transcript_quote(client: httpx.AsyncClient, transcript: str) -> str:
    """Pick the most quotable verbatim span of a long transcript for a quoted
    post. Asks vLLM to copy the best contiguous phrase, then verifies the copy
    really is a substring of the transcript (normalized) — a fabricated 'quote'
    can't slip through; on any failure falls back to the transcript's opening
    words."""
    try:
        r = await client.post(
            f"{settings.VLLM_URL}/v1/chat/completions",
            json={
                "model": settings.VLLM_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": "You select quotes from transcripts. Output only text copied word-for-word from the transcript — no commentary, no quote marks.",
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Copy the single most interesting, funny, or quotable contiguous "
                            f"phrase or sentence from this transcript, word-for-word. At most "
                            f"{_QUOTE_MAX_WORDS} words. Output only the copied words.\n\n"
                            f"Transcript: {transcript[:600]}"
                        ),
                    },
                ],
                "max_tokens": 80,
                "temperature": 0.3,
            },
        )
        if r.status_code == 200:
            cand = r.json()["choices"][0]["message"]["content"].strip()
            cand = cand.split("\n")[0].strip().strip('"“”')
            words = cand.split()
            if 3 <= len(words) <= _QUOTE_MAX_WORDS + 6 and _norm_quote_text(cand) in _norm_quote_text(transcript):
                return cand
    except Exception:
        pass
    words = transcript.split()
    quote = " ".join(words[:_QUOTE_MAX_WORDS])
    return quote + ("…" if len(words) > _QUOTE_MAX_WORDS else "")


def _is_junk_title(title: str) -> bool:
    """True if a streamer-supplied title is too thin for vLLM to work with —
    e.g. '1', '.', 'asdf123' typed just to get the clip creation flow started."""
    title = title.strip()
    return len(title) < 3 or not any(c.isalpha() for c in title)


async def _generate_title(client: httpx.AsyncClient, transcript: str, streamer: str) -> str:
    """Ask vLLM for a short clip title when the streamer-supplied one is junk."""
    try:
        r = await client.post(
            f"{settings.VLLM_URL}/v1/chat/completions",
            json={
                "model": settings.VLLM_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": "You write short, punchy titles for gaming clips. Output ONLY the title text — no labels, no quotes, no hashtags.",
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Write a short catchy title (under 60 characters) for this Twitch clip "
                            f"based on the transcript. Stay grounded in the transcript, do not invent facts. "
                            f"Streamer: {streamer}. Transcript: {transcript[:600]}"
                        ),
                    },
                ],
                "max_tokens": 30,
                "temperature": 0.7,
            },
        )
        if r.status_code == 200:
            raw = r.json()["choices"][0]["message"]["content"].strip()
            raw = raw.split("\n")[0].strip()
            if raw.startswith('"') and raw.endswith('"'):
                raw = raw[1:-1]
            return raw[:100].strip()
    except Exception:
        pass
    return ""


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


# Matches the "\n\nTwitch streaming ..."/"\n\nKick streaming ..." suffix _build_tweet
# appends, so _prepend_intro can trim the body instead of the suffix if needed.
_PLATFORM_SUFFIX_RE = re.compile(r'\n\n(?:Twitch|Kick) streaming .*$', re.DOTALL)


def _prepend_intro(intro: str, tweet_text: str) -> str:
    """Prepend a publish-time intro line to an already-built tweet, re-trimming
    the body (never the intro or the fixed platform suffix) if needed to stay
    under 280 chars — tweet_text may also carry operator edits from the approval
    UI, which this must not clobber beyond what's necessary to fit."""
    combined = f"{intro}\n\n{tweet_text}"
    if len(combined) <= 280:
        return combined

    m = _PLATFORM_SUFFIX_RE.search(tweet_text)
    suffix = m.group(0) if m else ""
    body = tweet_text[:m.start()] if m else tweet_text

    max_body = max(280 - len(intro) - 2 - len(suffix), 0)
    if len(body) > max_body:
        body = body[:max(max_body - 1, 0)].rstrip() + "…"
    return f"{intro}\n\n{body}{suffix}"


def _format_srt_timestamp(seconds: float) -> str:
    total_ms = int(round(max(seconds, 0.0) * 1000))
    hours, rem = divmod(total_ms, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    secs, ms = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{ms:03d}"


# Whisper's segment timestamps can span 50+ seconds of pause-free streamer speech,
# which X renders as one giant on-screen caption block (#153). Cap each cue and
# interpolate timing across words so CC appears roughly as spoken.
_SRT_MAX_CUE_SECONDS = 6.0
_SRT_MAX_CUE_CHARS = 84


def _split_srt_cue(text: str, start: float, end: float) -> list[tuple[str, float, float]]:
    """Split one Whisper chunk into cues no longer than _SRT_MAX_CUE_SECONDS /
    _SRT_MAX_CUE_CHARS, distributing the chunk's time span across the pieces
    proportionally to their character length (Whisper only gave us segment-level
    timing, so per-word timing is a linear estimate)."""
    duration = max(end - start, 0.0)
    if duration <= _SRT_MAX_CUE_SECONDS and len(text) <= _SRT_MAX_CUE_CHARS:
        return [(text, start, end)]
    secs_per_char = duration / len(text) if text else 0.0
    pieces: list[tuple[str, int]] = []  # (piece text, char weight)
    cur: list[str] = []
    cur_chars = 0
    for word in text.split():
        added = len(word) + (1 if cur else 0)
        if cur and (
            cur_chars + added > _SRT_MAX_CUE_CHARS
            or (cur_chars + added) * secs_per_char > _SRT_MAX_CUE_SECONDS
        ):
            pieces.append((" ".join(cur), cur_chars))
            cur, cur_chars = [], 0
            added = len(word)
        cur.append(word)
        cur_chars += added
    if cur:
        pieces.append((" ".join(cur), cur_chars))
    total_chars = sum(chars for _, chars in pieces) or 1
    cues = []
    t = start
    for i, (piece, chars) in enumerate(pieces):
        piece_end = end if i == len(pieces) - 1 else t + duration * chars / total_chars
        cues.append((piece, t, piece_end))
        t = piece_end
    return cues


def _build_srt(chunks: list[dict]) -> str:
    """Turn Whisper's return_timestamps=True chunks ([{"text", "timestamp": (start, end)}])
    into standard SRT. A chunk's end can be None (cut off mid-word); fall back to the
    prior chunk's end plus a small pad rather than dropping the cue. Long chunks are
    split into short cues via _split_srt_cue."""
    lines = []
    prev_end = 0.0
    idx = 0
    for chunk in chunks:
        text = (chunk.get("text") or "").strip()
        if not text:
            continue
        start, end = chunk.get("timestamp", (None, None))
        start = start if start is not None else prev_end
        end = end if end is not None else start + 2.0
        prev_end = end
        for cue_text, cue_start, cue_end in _split_srt_cue(text, start, end):
            idx += 1
            lines.append(str(idx))
            lines.append(f"{_format_srt_timestamp(cue_start)} --> {_format_srt_timestamp(cue_end)}")
            lines.append(cue_text)
            lines.append("")
    return "\n".join(lines)


async def process_clip(clip: dict) -> dict:
    """Transcribe clip audio with Whisper, generate caption with vLLM.
    Returns enriched clip dict ready to publish to processed_clips."""
    clip_path = clip.get("clip_path", "")
    if not clip_path or not Path(clip_path).exists():
        return {**clip, "transcript": "", "caption": "", "error": f"File not found: {clip_path}"}

    title = clip.get("title", "").strip()

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
                data = r.json()
                transcript = data.get("text", "").strip()
                chunks = data.get("chunks") or []
                if chunks:
                    try:
                        srt_path = Path(clip_path).with_suffix(".srt")
                        srt_path.write_text(_build_srt(chunks), encoding="utf-8")
                    except Exception as e:
                        print(f"[process_clip] srt build failed for {clip_path}: {e}")
        except Exception as e:
            transcript = f"[transcription error: {e}]"
        finally:
            wav_path.unlink(missing_ok=True)

        # Streamers often type filler ("1", ".") just to get the clip flow started —
        # backfill a real title from the transcript instead of disqualifying the clip.
        has_transcript = bool(transcript) and not transcript.startswith("[")
        if _is_junk_title(title) and has_transcript:
            title = await _generate_title(client, transcript, clip.get("streamer", "unknown")) or title

        if _is_junk_title(title):
            return {**clip, "title": title, "transcript": transcript, "caption": "", "error": "disqualified: no title"}

        # vLLM caption generation
        caption = ""
        error = ""
        caption_mode = ""
        quote_reason = ""
        if has_transcript and len(transcript.split()) <= _SHORT_TRANSCRIPT_WORDS:
            streamer_name = clip.get("streamer", "unknown")
            x_handle = get_x_handle(clip.get("streamer", ""))
            quoted = _quoted_caption(transcript, streamer_name)
            caption = _build_tweet(quoted, clip.get("source", "twitch"), clip.get("streamer", ""), x_handle)
            caption_mode, quote_reason = "quoted", "short transcript"
        elif has_transcript:
            streamer_name = clip.get("streamer", "unknown")
            # Comprehension gate (#175): if the model can't say what the streamer
            # is talking about, don't let the persona prompt force a reaction it
            # will have to fabricate — post the streamer's own words instead.
            topic = await _comprehend_topic(client, transcript, streamer_name)
            if not topic:
                quote_reason = "topic unclear"
            if topic:
                opener_style = random.choice(_CAPTION_OPENER_STYLES).format(name=streamer_name)
                # A different random subset of the example pool per call, not the same
                # fixed examples every time — see _CAPTION_EXAMPLES for why.
                example_lines = "\n".join(f"- \"{ex}\"" for ex in random.sample(_CAPTION_EXAMPLES, 4))
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are a cocky, trash-talking gaming chat regular live-reacting to clips — "
                            "funny, arrogant, a little trollish, but never boring. Sometimes you hype the "
                            "streamer up like they're the main character and everyone else should log off; "
                            "sometimes you roast them for what just happened in the clip. Pick whichever "
                            "fits the clip. Follow every rule exactly:\n"
                            f"1. The streamer's name is \"{streamer_name}\" — refer to them ONLY by this "
                            f"exact name. Never use he, she, him, her, his, or hers for {streamer_name}, "
                            f"even if the name sounds gendered to you, and even if the transcript uses a "
                            f"pronoun for someone else in the clip. You do not know {streamer_name}'s "
                            "gender and must not guess it.\n"
                            "2. Output ONLY the reaction sentence(s) — no labels, no headers, no markdown, "
                            "no quotes around the whole thing. Before you answer, check your first few "
                            f"words: if they are \"{streamer_name} just\" or \"{streamer_name}'s\" or any "
                            "other name-first-verb-second pattern, throw that draft out and rewrite with a "
                            "different opener. Required: open with a callout, a mock-shocked aside, a "
                            "direct jab, a bragging comparison, or a quote lead-in — never with the "
                            "streamer's name as the very first word.\n"
                            "3. Stay 100% grounded in the transcript. Never invent names, people, items, "
                            "or events that are not in it. If you put words inside quotation marks, they "
                            "must be copied word-for-word from the transcript — never invent a quote.\n"
                            "4. Exactly 1 emoji. No hashtags. No @ mentions. No links or URLs.\n"
                            "5. Keep it funny and a little cocky/trollish — teasing the streamer or "
                            "trash-talking on their behalf is encouraged. No slurs or hate speech.\n"
                            "6. Under 200 characters.\n\n"
                            "Examples of the range of openers/tone to draw from (do not reuse these "
                            "verbatim, and don't let any one of them become your default template):\n"
                            f"{example_lines}\n"
                            "Example of what NOT to do: \"She just clutched a 1v5, how does she do it?!\" "
                            "— wrong, this guesses gender from the name instead of using it directly."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"React to this clip by {streamer_name} like you're a cocky regular in "
                            f"their Twitch chat. Quote or paraphrase something actually said in the "
                            f"transcript, or roast/hype them over what just happened. "
                            f"What {streamer_name} is talking about in this clip: {topic} "
                            f"If that isn't about gameplay, react to what it's actually about — do "
                            f"not invent gameplay events (kills, clutches, 1v3s) that are not in "
                            f"the transcript. "
                            f"For this one, your opener MUST be: {opener_style}. Do not start with "
                            f"\"{streamer_name} just\" or \"{streamer_name}'s\" — use the required opener "
                            f"style instead. "
                            f"Clip title: '{title}'. "
                            f"Transcript: {transcript[:600]}\n\n"
                            f"Remember: call them {streamer_name}, never a pronoun."
                        ),
                    },
                ]

                # Gendered-pronoun violations get corrective retries before disqualifying —
                # the LLM is told exactly what it did wrong and asked to redo it, rather than
                # the clip just being thrown away on the first slip (2026-07-23, all streamers;
                # bumped to 3 retries same day per Steven's ask).
                max_attempts = 4
                for attempt in range(max_attempts):
                    try:
                        r = await client.post(
                            f"{settings.VLLM_URL}/v1/chat/completions",
                            json={
                                "model": settings.VLLM_MODEL,
                                "messages": messages,
                                "max_tokens": 120,
                                "temperature": 0.7,
                                "frequency_penalty": 0.6,
                                "presence_penalty": 0.3,
                            },
                        )
                        if r.status_code == 200:
                            raw = r.json()["choices"][0]["message"]["content"]
                            cleaned = _clean_caption(raw)
                            if not cleaned:
                                error = "disqualified: empty caption after cleaning"
                                break
                            elif _has_degenerate_repetition(cleaned):
                                error = "disqualified: degenerate repeated output"
                                break
                            elif _has_gendered_pronoun(cleaned):
                                error = f"disqualified: gendered pronoun used for {streamer_name}"
                                if attempt < max_attempts - 1:
                                    messages.append({"role": "assistant", "content": raw})
                                    messages.append({
                                        "role": "user",
                                        "content": (
                                            f"That used a gendered pronoun (he/him/his/she/her/hers) for "
                                            f"{streamer_name}. You do not know {streamer_name}'s gender. Rewrite "
                                            f"it — same rules as before, but refer to them ONLY as "
                                            f"\"{streamer_name}\", zero pronouns anywhere in the sentence."
                                        ),
                                    })
                                    continue
                                break
                            elif _has_fabricated_quote(cleaned, transcript):
                                # Quote-marked words that aren't in the transcript — the
                                # model invented a quote (#175). Post a real one instead.
                                quote_reason = "fabricated quote"
                                break
                            else:
                                # No "watching live" intro baked in here — a clip can sit
                                # in the pending queue for hours before it's actually
                                # published, so whether that framing is still honest is
                                # decided at publish time instead (see publish_clip()).
                                x_handle = get_x_handle(clip.get("streamer", ""))
                                caption = _build_tweet(
                                    cleaned, clip.get("source", "twitch"), clip.get("streamer", ""), x_handle
                                )
                                caption_mode = "reaction"
                                error = ""
                                break
                        else:
                            error = f"caption error: vLLM returned {r.status_code}"
                            break
                    except Exception as e:
                        error = f"caption error: {e}"
                        break

            if quote_reason and not caption:
                # The AI couldn't figure out what the streamer is talking about
                # (gate) or proved it by inventing a quote (check above) — post
                # the streamer's own words instead of an AI reaction (#175).
                quote = await _extract_transcript_quote(client, transcript)
                x_handle = get_x_handle(clip.get("streamer", ""))
                caption = _build_tweet(
                    _quoted_caption(quote, streamer_name), clip.get("source", "twitch"),
                    clip.get("streamer", ""), x_handle,
                )
                caption_mode = "quoted"
                error = ""
                print(f"[process_clip] quoted-post fallback ({quote_reason}) for {clip.get('clip_id', clip_path)}")
        else:
            error = "disqualified: no transcript"

        if error and not caption:
            return {**clip, "title": title, "transcript": transcript, "caption": "", "error": error}

    return {
        **clip, "title": title, "transcript": transcript, "caption": caption,
        "caption_mode": caption_mode, "quote_reason": quote_reason,
    }


# ── Clip queue ────────────────────────────────────────────────────────────────

async def clip_queue(limit: int = 20) -> list[dict]:
    """Peek the last `limit` records from processed_clips.

    Uses getmany() (direct fetch) instead of the async iterator so that
    manual seek() works reliably — the async for iterator hangs after seek()
    in aiokafka when there are no in-flight fetch requests queued.

    getmany() is a one-shot poll, not a guaranteed drain of the requested
    range — after seek() it can return with only a few of the messages up to
    the broker's current position, well short of `limit`, even though the
    rest are readily available on the next poll. Confirmed live 2026-07-24:
    a single getmany() call after seeking to the last 20 offsets returned
    only 2 messages (both already `pending`), silently hiding 13 real,
    unpublished, ready-to-review clips that a second poll picked up
    immediately. Loop until the consumer's position catches up to the known
    end offset (or a bounded number of polls, in case something's actually
    stalled) instead of trusting one call to have delivered everything.
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

        start = max(0, end - limit)
        consumer.seek(tp, start)

        # Poll until caught up to `end` — a single getmany() isn't a
        # guaranteed drain of the sought range, see docstring above.
        messages: dict[int, object] = {}
        for _ in range(8):
            batch = await asyncio.wait_for(
                consumer.getmany(tp, timeout_ms=3000, max_records=limit),
                timeout=10.0,
            )
            for msg in batch.get(tp, []):
                messages[msg.offset] = msg
            if await consumer.position(tp) >= end:
                break

        skipped = get_skipped()
        published = get_published()
        pending = {p["clip_id"] for p in _load_pending()}
        for offset in sorted(messages):
            msg = messages[offset]
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

def _trim_if_oversized(
    path: Path, max_duration: float = MAX_TWEET_VIDEO_DURATION
) -> tuple[float | None, bool, str]:
    """Probe path's real duration; if it exceeds max_duration, re-encode trim it
    in place. Returns (duration_before, trimmed, error) — error is empty
    whenever trimmed is True or no trim was needed.

    Re-encodes rather than -c copy: stream-copy trims can only cut at the nearest
    keyframe, which can overshoot the 5s safety margin and land back over X's 120s
    hard limit.
    """
    duration = _probe_video_duration(path)
    if duration is None or duration <= max_duration:
        return duration, False, ""
    trimmed = path.with_suffix(".trimmed.mp4")
    try:
        # Serialized via _overlay_lock like _burn_platform_overlay/_burn_glitch_intro —
        # without it this can run concurrently with one of those (or another trim) on
        # this pod's 1-CPU/1Gi limit and get OOM-killed (returncode -9). Confirmed live
        # 2026-07-29: an unlocked trim of a 1920x1240 clip got SIGKILLed at ~77s while
        # FetchClips was mid-burst. Timeout raised from 90s to 240s to match — at this
        # pod's measured single-threaded veryfast encode speed (~0.6x realtime),
        # encoding a full MAX_TWEET_VIDEO_DURATION-length output alone takes ~190s.
        with _overlay_lock():
            r = subprocess.run(
                # x264 auto-detects thread count from the host's visible CPU count, not
                # this pod's 1-CPU/1Gi limit (k8s/deployment.yaml) — at 24 threads on a
                # 1920x1240 frame, per-thread encode buffers blow the memory limit and
                # the kernel OOM-kills ffmpeg (returncode -9) within ~1s every time.
                # Matches the same thread cap already used by _burn_platform_overlay and
                # _burn_glitch_intro's encode_still for the identical reason.
                ["ffmpeg", "-y", "-i", str(path), "-t", str(max_duration),
                 "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
                 "-threads", "1", "-x264opts", "threads=1:sliced-threads=0",
                 "-c:a", "aac", str(trimmed)],
                capture_output=True, timeout=240,
            )
    except subprocess.TimeoutExpired as e:
        trimmed.unlink(missing_ok=True)
        return duration, False, f"ffmpeg timed out after 240s: {(e.stderr or b'').decode(errors='replace')[-800:]}"
    out_size = trimmed.stat().st_size if trimmed.exists() else -1
    if r.returncode == 0 and trimmed.exists() and out_size > 10_000:
        trimmed.replace(path)
        return duration, True, ""
    trimmed.unlink(missing_ok=True)
    # ffmpeg's default -stats progress output is many repeated \r-updated "frame="
    # lines that drown out the actual error — drop those but keep the final one,
    # since it shows how much was actually encoded before ffmpeg stopped.
    stderr_text = r.stderr.decode(errors="replace")
    lines = stderr_text.replace("\r", "\n").splitlines()
    frame_lines = [ln for ln in lines if ln.lstrip().startswith("frame=")]
    non_progress = [ln for ln in lines if ln.strip() and not ln.lstrip().startswith("frame=")]
    tail = (frame_lines[-1:] if frame_lines else []) + non_progress[-20:]
    summary = f"[returncode={r.returncode} out_size={out_size}] " + " | ".join(tail)
    return duration, False, summary[:1500]


def _publish_sync(clip_path: str, tweet_text: str) -> dict:
    import tweepy

    if not all([settings.X_API_KEY, settings.X_API_SECRET,
                settings.X_ACCESS_TOKEN, settings.X_ACCESS_TOKEN_SECRET]):
        raise RuntimeError("X API credentials not configured")

    path = Path(clip_path)
    if not path.exists():
        raise FileNotFoundError(f"Clip not found: {clip_path}")

    duration, trimmed, err = _trim_if_oversized(path)
    if duration is not None and duration > MAX_TWEET_VIDEO_DURATION:
        if trimmed:
            print(f"[_publish_sync] trimmed {path.name}: {duration:.1f}s -> "
                  f"{MAX_TWEET_VIDEO_DURATION:.1f}s (exceeded X's 2min limit)")
        else:
            raise RuntimeError(
                f"trim failed for {path.name} ({duration:.1f}s, exceeds "
                f"{MAX_TWEET_VIDEO_DURATION:.0f}s X limit): {err}"
            )

    auth = tweepy.OAuth1UserHandler(
        settings.X_API_KEY,
        settings.X_API_SECRET,
        settings.X_ACCESS_TOKEN,
        settings.X_ACCESS_TOKEN_SECRET,
    )
    api_v1 = tweepy.API(auth)
    media = api_v1.media_upload(str(path), chunked=True, media_category="TweetVideo")

    srt_path = path.with_suffix(".srt")
    if srt_path.exists():
        try:
            subtitle_media = api_v1.chunked_upload(
                str(srt_path), file_type="text/srt", media_category="Subtitles",
            )
            api_v1.request(
                "POST", "media/subtitles/create",
                json_payload={
                    "media_id": media.media_id_string,
                    "media_category": "TweetVideo",
                    "subtitle_info": {
                        "subtitles": [{
                            "media_id": subtitle_media.media_id_string,
                            "language_code": "en",
                            "display_name": "English",
                        }],
                    },
                },
                upload_api=True,
            )
        except Exception as e:
            print(f"[_publish_sync] subtitle attach failed for {path.name}: {e}")

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


async def publish_clip(
    clip_path: str, tweet_text: str, clip_id: str = "", title: str = "",
    source: str = "", streamer: str = "", url: str = "",
    thumbnail_url: str = "", x_handle: str = "",
) -> dict:
    # Decided here, at the last possible moment before the actual X post, not
    # back at process_clip() time — a clip can sit in the pending queue for
    # hours, so "watching live now" framing baked in early can go stale.
    # is_streamer_live is a real-time check made right now, so its answer is
    # ground truth regardless of how old the underlying clip capture is. Any
    # failure here (live-check error) falls back to False, which _choose_intro
    # treats as the honest "missed it" framing rather than risking a false
    # live claim.
    is_live = False
    if streamer:
        entry = f"kick:{streamer}" if source == "kick" else streamer
        async with httpx.AsyncClient(verify=settings.NIFI_VERIFY_TLS, timeout=15.0) as live_client:
            is_live = await is_streamer_live(live_client, entry)
    intro = _choose_intro(streamer or "them", is_live)
    final_text = _prepend_intro(intro, tweet_text)

    result = await asyncio.to_thread(_publish_sync, clip_path, final_text)
    if result.get("ok") and clip_id:
        mark_published(
            clip_id, title, source, streamer, url, thumbnail_url, x_handle,
            result.get("tweet_id", ""), result.get("url", ""),
        )
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
            seek_start = max(begin, end - 5)
            consumer.seek(tp, seek_start)
            # getmany() is a one-shot poll, not a guaranteed drain -- loop until
            # caught up to `end` instead of trusting one call for all 5 records.
            # Same underlying issue as clip_queue()'s fix, 2026-07-24.
            messages: dict[int, object] = {}
            for _ in range(8):
                batch = await asyncio.wait_for(
                    consumer.getmany(tp, timeout_ms=2000, max_records=5),
                    timeout=8.0,
                )
                for msg in batch.get(tp, []):
                    messages[msg.offset] = msg
                if await consumer.position(tp) >= end:
                    break
            for offset in sorted(messages):
                msg = messages[offset]
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

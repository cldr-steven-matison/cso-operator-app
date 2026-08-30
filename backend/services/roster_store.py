"""Streamers roster/catalog store — Postgres-backed, cached in-process (#275).

Replaces the hardcoded ``_TWITCH_LOGINS`` / ``_KICK_LOGINS`` / ``_STREAMER_CATALOG``
/ ``_STREAMER_PATH_OVERRIDES`` constants in ``services/streamers.py`` as the source
of truth, so a mod-only chat command (#273) can add/remove a streamer durably
instead of it vanishing on the next pod restart.

Shape:

* One table, ``streamer`` (see ``_SCHEMA``), one row per (platform, login).
  ``active=false`` is the soft-delete a chat ``➖`` performs — history is kept.
* The roster is read on hot paths (every chat trigger, every post's
  ``get_x_handle``, the live-status poll) by dozens of *synchronous* callers, so
  those callers never touch the pool. They read ``_cache``, which is loaded once
  at startup and refreshed after every write here. A cache miss (DB down, creds
  absent) falls back to the hardcoded constants — exactly today's behaviour.
* The hardcoded constants double as the idempotent seed: ``ensure_schema_and_seed``
  creates the table if missing and inserts any seed row that isn't already there
  (``ON CONFLICT DO NOTHING``), so a fresh DB comes up with today's roster and an
  existing one is never overwritten.

Pool sizing is small on purpose: writes are rare (a mod typing a command) and the
reads are served from the cache.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import asyncpg

from config import settings

log = logging.getLogger(__name__)

X_HANDLE_CONFIRMED = "confirmed"
X_HANDLE_NEEDS_REVIEW = "needs_review"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS streamer (
    seq              bigserial   NOT NULL,   -- insertion order: seed declaration order, then adds
    platform         text        NOT NULL CHECK (platform IN ('twitch', 'kick')),
    login            text        NOT NULL,
    x_handle         text,
    x_handle_status  text        CHECK (x_handle_status IN ('confirmed', 'needs_review')),
    clip_enabled     boolean     NOT NULL DEFAULT true,
    gif_enabled      boolean     NOT NULL DEFAULT true,
    gif_post_enabled boolean     NOT NULL DEFAULT false,
    active           boolean     NOT NULL DEFAULT true,
    added_by         text,
    added_at         timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    source           text        NOT NULL DEFAULT 'seed',
    PRIMARY KEY (platform, login)
);
"""


@dataclass(frozen=True)
class Streamer:
    platform: str          # "twitch" | "kick"
    login: str             # bare, lowercase
    x_handle: str          # no "@"; "" when unknown
    x_handle_status: str   # "confirmed" | "needs_review" | ""
    clip: bool
    gif: bool
    gif_post: bool
    active: bool

    @property
    def entry(self) -> str:
        """The ``login`` / ``kick:login`` shape the rest of the module uses."""
        return f"kick:{self.login}" if self.platform == "kick" else self.login


_pool: asyncpg.Pool | None = None
_cache: dict[tuple[str, str], Streamer] | None = None   # None = never loaded
_lock = asyncio.Lock()


def enabled() -> bool:
    return bool(settings.STREAMERS_DB_USER)


def loaded() -> bool:
    return _cache is not None


# ── Lifecycle ───────────────────────────────────────────────────────────────

async def start(seed: dict) -> None:
    """Open the pool, ensure the schema, seed from the hardcoded constants if
    needed, and load the cache. Never raises: a DB problem logs and leaves the
    cache unloaded, so every reader falls back to the constants."""
    global _pool
    if not enabled():
        log.info("roster_store disabled (STREAMERS_DB_USER unset) — using hardcoded roster")
        return
    try:
        _pool = await asyncpg.create_pool(
            host=settings.STREAMERS_DB_HOST, port=settings.STREAMERS_DB_PORT,
            database=settings.STREAMERS_DB_NAME, user=settings.STREAMERS_DB_USER,
            password=settings.STREAMERS_DB_PASSWORD, min_size=0, max_size=2,
        )
        await ensure_schema_and_seed(seed)
        await reload()
        log.info("roster_store loaded %d streamers from Postgres", len(_cache or {}))
    except Exception as e:  # noqa: BLE001 — degrade to the constants, never fail startup
        log.error("roster_store unavailable, falling back to hardcoded roster: %s", e)


async def stop() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def ensure_schema_and_seed(seed: dict) -> int:
    """Idempotent. ``seed`` is ``{"twitch": [login...], "kick": [login...],
    "x_handles": {login: handle}, "paths": {login: {"clip","gif","gif_post"}}}``
    — the exact hardcoded constants. Returns the number of rows inserted."""
    assert _pool is not None
    x_handles = {k.lower(): v for k, v in seed.get("x_handles", {}).items()}
    paths = {k.lower(): v for k, v in seed.get("paths", {}).items()}
    rows = []
    for platform in ("twitch", "kick"):
        for login in seed.get(platform, []):
            login = login.lower()
            p = paths.get(login, {})
            handle = x_handles.get(login, "")
            rows.append((platform, login, handle or None,
                         X_HANDLE_CONFIRMED if handle else None,
                         p.get("clip", True), p.get("gif", True), p.get("gif_post", False)))
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA)
        inserted = 0
        for row in rows:
            status = await conn.execute(
                "INSERT INTO streamer (platform, login, x_handle, x_handle_status, "
                "clip_enabled, gif_enabled, gif_post_enabled, source) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, 'seed') "
                "ON CONFLICT (platform, login) DO NOTHING", *row)
            inserted += int(status.endswith(" 1"))
    if inserted:
        log.info("roster_store seeded %d streamer rows", inserted)
    return inserted


async def reload() -> None:
    """Refresh the cache from Postgres. Called after every write. Rows come back
    in ``seq`` order so ``get_roster()`` keeps the hardcoded declaration order the
    rest of the app (and its UI) has always seen, with chat adds appended."""
    global _cache
    assert _pool is not None
    async with _pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT platform, login, x_handle, x_handle_status, clip_enabled, "
            "gif_enabled, gif_post_enabled, active FROM streamer ORDER BY seq")
    _cache = {
        (r["platform"], r["login"]): Streamer(
            platform=r["platform"], login=r["login"],
            x_handle=r["x_handle"] or "", x_handle_status=r["x_handle_status"] or "",
            clip=r["clip_enabled"], gif=r["gif_enabled"], gif_post=r["gif_post_enabled"],
            active=r["active"])
        for r in records
    }


# ── Cached reads (sync — safe from any caller) ──────────────────────────────
# Each returns None when the cache was never loaded so the caller can fall back
# to its hardcoded constant; an *empty* result from a loaded cache is real.

def logins(platform: str) -> list[str] | None:
    if _cache is None:
        return None
    return [s.login for s in _cache.values() if s.platform == platform and s.active]


def get(login: str, platform: str | None = None, *,
        include_inactive: bool = False) -> Streamer | None:
    """Row for a bare login — on the given platform, or the first platform that
    has it (a login is unique per platform; the catalog conventions key handles
    and paths on the bare login). Active rows only unless ``include_inactive``."""
    if _cache is None:
        return None
    login = login.lower()
    for p in ((platform,) if platform else ("twitch", "kick")):
        s = _cache.get((p, login))
        if s is not None and (s.active or include_inactive):
            return s
    return None


# ── Writes (async; each reloads the cache) ─────────────────────────────────

async def add(platform: str, login: str, *, x_handle: str = "",
              x_handle_status: str = "", added_by: str = "", source: str = "chat") -> Streamer:
    """Insert or re-activate a streamer. Re-adding an existing row flips
    ``active`` back on and keeps its handle/paths unless a new handle is given."""
    assert _pool is not None
    login = login.lower().lstrip("@")
    async with _lock:
        async with _pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO streamer (platform, login, x_handle, x_handle_status, added_by, source) "
                "VALUES ($1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (platform, login) DO UPDATE SET "
                "  active = true, updated_at = now(), "
                "  x_handle = COALESCE(EXCLUDED.x_handle, streamer.x_handle), "
                "  x_handle_status = COALESCE(EXCLUDED.x_handle_status, streamer.x_handle_status)",
                platform, login, x_handle or None, x_handle_status or None,
                added_by or None, source)
        await reload()
    s = _cache.get((platform, login)) if _cache else None
    assert s is not None
    return s


async def remove(platform: str, login: str) -> bool:
    """Soft-delete. Returns False when there was no active row to remove."""
    assert _pool is not None
    login = login.lower().lstrip("@")
    async with _lock:
        async with _pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE streamer SET active = false, updated_at = now() "
                "WHERE platform = $1 AND login = $2 AND active", platform, login)
        await reload()
    return status.endswith(" 1")


async def set_x_handle(platform: str, login: str, x_handle: str,
                       status: str = X_HANDLE_CONFIRMED) -> bool:
    assert _pool is not None
    async with _lock:
        async with _pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE streamer SET x_handle = $3, x_handle_status = $4, updated_at = now() "
                "WHERE platform = $1 AND login = $2",
                platform, login.lower().lstrip("@"), x_handle.lstrip("@") or None,
                status or None)
        await reload()
    return result.endswith(" 1")

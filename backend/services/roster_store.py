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

# Identity columns for the DGX Spark caption brain (#276) — additive, idempotent,
# re-run at every startup. Pronouns are typed by a human in the app's Watchlist
# tab (#279) and never inferred; the brain reads only confirmed ones, through the
# ``streamer_brain`` view below, which is the whole contract the Spark consumes.
_MIGRATIONS = """
ALTER TABLE streamer ADD COLUMN IF NOT EXISTS display_name    text;
ALTER TABLE streamer ADD COLUMN IF NOT EXISTS aliases         text[] NOT NULL DEFAULT '{}';
ALTER TABLE streamer ADD COLUMN IF NOT EXISTS pronouns        text;
ALTER TABLE streamer ADD COLUMN IF NOT EXISTS pronouns_status text
    CHECK (pronouns_status IN ('confirmed', 'needs_review'));
ALTER TABLE streamer ADD COLUMN IF NOT EXISTS notes           text;
CREATE OR REPLACE VIEW streamer_brain AS
SELECT CASE WHEN platform = 'kick' THEN 'kick:' || login ELSE login END AS streamer_key,
       platform, login,
       COALESCE(display_name, login)                       AS display_name,
       aliases, x_handle,
       (x_handle_status = 'confirmed')                     AS x_handle_confirmed,
       CASE WHEN pronouns_status = 'confirmed' THEN pronouns END AS pronouns,
       (pronouns_status = 'confirmed')                     AS pronouns_confirmed,
       notes, active
FROM streamer;
"""

PRONOUNS_CONFIRMED = "confirmed"
PRONOUNS_NEEDS_REVIEW = "needs_review"

_COLUMNS = ("platform", "login", "x_handle", "x_handle_status", "clip_enabled",
            "gif_enabled", "gif_post_enabled", "active", "added_by", "added_at",
            "updated_at", "source", "display_name", "aliases", "pronouns",
            "pronouns_status", "notes")


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
    # Bookkeeping + the #276 identity columns (all "" / () when unset).
    added_by: str = ""
    added_at: str = ""          # ISO-8601, "" when unknown
    updated_at: str = ""
    source: str = "seed"
    display_name: str = ""
    aliases: tuple[str, ...] = ()
    pronouns: str = ""
    pronouns_status: str = ""   # "confirmed" | "needs_review" | ""
    notes: str = ""

    @property
    def entry(self) -> str:
        """The ``login`` / ``kick:login`` shape the rest of the module uses."""
        return f"kick:{self.login}" if self.platform == "kick" else self.login

    def as_dict(self) -> dict:
        """JSON-ready row for the app's roster endpoints (#279)."""
        return {
            "platform": self.platform, "login": self.login, "entry": self.entry,
            "x_handle": self.x_handle, "x_handle_status": self.x_handle_status,
            "clip_enabled": self.clip, "gif_enabled": self.gif, "gif_post_enabled": self.gif_post,
            "active": self.active, "added_by": self.added_by, "added_at": self.added_at,
            "updated_at": self.updated_at, "source": self.source,
            "display_name": self.display_name, "aliases": list(self.aliases),
            "pronouns": self.pronouns, "pronouns_status": self.pronouns_status,
            "notes": self.notes,
        }


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
        await conn.execute(_MIGRATIONS)
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
            f"SELECT {', '.join(_COLUMNS)} FROM streamer ORDER BY seq")
    _cache = {
        (r["platform"], r["login"]): Streamer(
            platform=r["platform"], login=r["login"],
            x_handle=r["x_handle"] or "", x_handle_status=r["x_handle_status"] or "",
            clip=r["clip_enabled"], gif=r["gif_enabled"], gif_post=r["gif_post_enabled"],
            active=r["active"],
            added_by=r["added_by"] or "",
            added_at=r["added_at"].isoformat() if r["added_at"] else "",
            updated_at=r["updated_at"].isoformat() if r["updated_at"] else "",
            source=r["source"] or "seed",
            display_name=r["display_name"] or "", aliases=tuple(r["aliases"] or ()),
            pronouns=r["pronouns"] or "", pronouns_status=r["pronouns_status"] or "",
            notes=r["notes"] or "")
        for r in records
    }


# ── Cached reads (sync — safe from any caller) ──────────────────────────────
# Each returns None when the cache was never loaded so the caller can fall back
# to its hardcoded constant; an *empty* result from a loaded cache is real.

def logins(platform: str) -> list[str] | None:
    if _cache is None:
        return None
    return [s.login for s in _cache.values() if s.platform == platform and s.active]


def list_all() -> list[Streamer] | None:
    """Every row, inactive included, in ``seq`` order — the Watchlist grid (#279)."""
    if _cache is None:
        return None
    return list(_cache.values())


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


async def hard_delete(platform: str, login: str) -> bool:
    """Really delete the row (test rows only — #279). Returns False when absent."""
    assert _pool is not None
    login = login.lower().lstrip("@")
    async with _lock:
        async with _pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM streamer WHERE platform = $1 AND login = $2", platform, login)
        await reload()
    return status.endswith(" 1")


# Columns the Watchlist grid may edit (#279). Everything else (login, platform,
# seq, added_*) is identity/bookkeeping and stays read-only from the UI.
_UPDATABLE = {"x_handle", "x_handle_status", "clip_enabled", "gif_enabled",
              "gif_post_enabled", "active", "display_name", "aliases", "pronouns",
              "pronouns_status", "notes"}


async def update(platform: str, login: str, **fields) -> Streamer | None:
    """Partial update of one row from the Watchlist grid. Unknown keys raise
    ``ValueError`` (a coding error, not user input); ``None`` for a nullable
    text column clears it. Returns the refreshed row, or None when absent."""
    assert _pool is not None
    bad = set(fields) - _UPDATABLE
    if bad:
        raise ValueError(f"not updatable: {sorted(bad)}")
    login = login.lower().lstrip("@")
    sets, args = [], [platform, login]
    for col, val in fields.items():
        if col == "x_handle" and isinstance(val, str):
            val = val.lstrip("@") or None
        elif col == "aliases":
            val = [a.strip().lstrip("@") for a in (val or []) if a and a.strip()]
        elif col in ("x_handle_status", "pronouns_status", "pronouns", "display_name", "notes"):
            val = (val.strip() if isinstance(val, str) else val) or None
        args.append(val)
        sets.append(f"{col} = ${len(args)}")
    if not sets:
        return get(login, platform, include_inactive=True)
    async with _lock:
        async with _pool.acquire() as conn:
            await conn.execute(
                f"UPDATE streamer SET {', '.join(sets)}, updated_at = now() "
                "WHERE platform = $1 AND login = $2", *args)
        await reload()
    return get(login, platform, include_inactive=True)


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

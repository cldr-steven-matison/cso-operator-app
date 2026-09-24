"""Review-queue store for the streamers-do surface — Postgres, replacing the
Kafka ``processed_clips`` read (#382).

On the home app (``ROLE=producer``) the review queue is ``clip_queue()``'s seek
over the ``processed_clips`` topic. The droplet has no Kafka: the home NiFi
``DOBridge`` tap consumes that same topic and ships each record over S2S, and
``nifi-do``'s ``DOIngest`` POSTs it to ``/api/streamers/ingest/record``, which
lands it here. ``clip_queue()`` on the surface reads these rows and applies the
exact same filters (file exists, not skipped/pending/published, has a caption).

Everything else about review — pending, published, skipped, the gif index and
verdicts — is still the atomic JSON under ``CLIP_STORAGE_PATH``, which on the
droplet sits on the block volume. Folding those into this table is later work.

Same lifecycle shape as ``roster_store``: never blocks or fails startup, and a
background loop reconnects with backoff. Uses the same ``STREAMERS_DB_*`` DB.
"""
from __future__ import annotations

import asyncio
import json
import logging

import asyncpg

from config import settings

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS clips (
    clip_id      text        PRIMARY KEY,              -- as in processed_clips
    kind         text        NOT NULL DEFAULT 'clip' CHECK (kind IN ('clip', 'gif')),
    record       jsonb       NOT NULL,                  -- the processed_clips value, paths droplet-local
    received_at  timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS clips_kind_received_idx ON clips (kind, received_at);
"""

_pool: asyncpg.Pool | None = None
_reconnect_task: asyncio.Task | None = None
_RETRY_MIN_S, _RETRY_MAX_S = 2.0, 30.0


def enabled() -> bool:
    return bool(settings.STREAMERS_DB_USER)


async def _connect() -> None:
    global _pool
    pool = await asyncpg.create_pool(
        host=settings.STREAMERS_DB_HOST, port=settings.STREAMERS_DB_PORT,
        database=settings.STREAMERS_DB_NAME, user=settings.STREAMERS_DB_USER,
        password=settings.STREAMERS_DB_PASSWORD, min_size=0, max_size=4,
    )
    try:
        async with pool.acquire() as conn:
            await conn.execute(_SCHEMA)
    except Exception:
        await pool.close()
        raise
    _pool = pool
    log.info("clip_store ready")


async def _reconnect_loop() -> None:
    delay = _RETRY_MIN_S
    while True:
        await asyncio.sleep(delay)
        try:
            await _connect()
            return
        except Exception as e:  # noqa: BLE001 — keep trying
            log.warning("clip_store still unavailable, retrying in %.0fs: %s", delay, e)
            delay = min(delay * 2, _RETRY_MAX_S)


async def start() -> None:
    global _reconnect_task
    if not enabled():
        log.warning("clip_store disabled (STREAMERS_DB_USER unset) — review queue will be empty")
        return
    try:
        await _connect()
    except Exception as e:  # noqa: BLE001 — never fail startup
        log.error("clip_store unavailable, retrying in the background: %s", e)
        _reconnect_task = asyncio.create_task(_reconnect_loop())


async def stop() -> None:
    global _pool, _reconnect_task
    if _reconnect_task is not None:
        _reconnect_task.cancel()
        _reconnect_task = None
    if _pool is not None:
        await _pool.close()
        _pool = None


def _require_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("clip_store not connected")
    return _pool


async def upsert(clip_id: str, kind: str, record: dict) -> None:
    """Insert or replace one record. A re-ingest (NiFi retry, a re-processed
    clip) replaces the record but keeps its original received_at, so the card
    doesn't jump to the end of the review queue."""
    await _require_pool().execute(
        """INSERT INTO clips (clip_id, kind, record) VALUES ($1, $2, $3::jsonb)
           ON CONFLICT (clip_id) DO UPDATE
             SET kind = EXCLUDED.kind, record = EXCLUDED.record, updated_at = now()""",
        clip_id, kind, json.dumps(record),
    )


async def review_records(kind: str = "clip") -> list[dict]:
    """Every stored record of ``kind``, oldest first, with ``_ts`` (epoch ms of
    arrival) standing in for the Kafka broker timestamp the UI sorts on. The
    caller filters; an unreachable DB raises rather than posing as empty."""
    rows = await _require_pool().fetch(
        "SELECT record, received_at FROM clips WHERE kind = $1 ORDER BY received_at",
        kind,
    )
    out = []
    for row in rows:
        record = json.loads(row["record"])
        record["_ts"] = int(row["received_at"].timestamp() * 1000)
        out.append(record)
    return out

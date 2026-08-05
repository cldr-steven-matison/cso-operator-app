"""Continuous per-streamer chat activity.

WatchlistChatSnapshotPoller (NiFi) re-runs inspector.inspect_chat() on a timer
per watchlisted Twitch channel and publishes each cycle's result verbatim to
Kafka (settings.TOPIC_CHAT_ACTIVITY) -- this module never captures chat
itself. It just consumes that topic, keeps a rolling snapshot + short history
per streamer, tracks which usernames show up as active chatters across
multiple watchlisted channels (the "hops between channels" bot-farm signal),
and persists both to plain JSON files under CLIP_STORAGE_PATH/.chat_activity/
-- same convention .watchlist.json etc already use -- so the Users/Bots page
survives a backend restart without going blank.

Two distinct consumer roles, deliberately not shared:
- start_aggregator(): one long-lived consumer, the only thing keeping durable
  state (persisted snapshots + cross-channel index).
- tail_streamer(): a short-lived per-connection consumer per open SSE tab,
  same shape as services/kafka.py's tail(), no persistence responsibility.
"""
import asyncio
import json
import logging
from collections import deque
from pathlib import Path
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer

from config import settings
from services.streamers import _atomic_write_json

logger = logging.getLogger(__name__)

_HISTORY_LEN = 20          # snapshots kept per streamer for the recent trend
_INDEX_FILENAME = "_chatter_index.json"

_latest: dict[str, dict] = {}         # streamer -> most recent enriched snapshot
_history: dict[str, deque] = {}       # streamer -> deque[snapshot], bounded
_chatter_index: dict[str, dict] = {}  # username -> {"channels": {streamer: {...}}}
_aggregator_task: asyncio.Task | None = None


def _activity_dir() -> Path:
    d = Path(settings.CLIP_STORAGE_PATH) / ".chat_activity"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _snapshot_path(streamer: str) -> Path:
    safe = streamer.replace("/", "_")
    return _activity_dir() / f"{safe}.json"


def _index_path() -> Path:
    return _activity_dir() / _INDEX_FILENAME


def _load_state() -> None:
    """Reload persisted snapshots + the cross-channel index on startup, so a
    backend restart doesn't blank the Users/Bots live view."""
    for f in _activity_dir().glob("*.json"):
        if f.name == _INDEX_FILENAME:
            continue
        try:
            snap = json.loads(f.read_text())
        except Exception:
            logger.warning("chat_activity: failed to load snapshot %s", f, exc_info=True)
            continue
        streamer = snap.get("login")
        if streamer:
            _latest[streamer] = snap
            _history[streamer] = deque([snap], maxlen=_HISTORY_LEN)

    idx_path = _index_path()
    if idx_path.exists():
        try:
            _chatter_index.update(json.loads(idx_path.read_text()))
        except Exception:
            logger.warning("chat_activity: failed to load chatter index", exc_info=True)


def _update_chatter_index(streamer: str, snapshot: dict, ts: float) -> None:
    """Track which channels each chatter/bot has been active in, and annotate
    the snapshot's own entries with how many *other* watchlisted channels that
    same username has recently shown up in -- accounts that hop between
    channels are a real bot-farm signal a single-channel view can't catch
    (adopted from reviewing botted.wtf's "Viewer Intel" tool)."""
    entries = snapshot.get("chatters", []) + snapshot.get("bots", [])
    for entry in entries:
        uname = entry.get("username")
        if not uname:
            continue
        record = _chatter_index.setdefault(uname, {"channels": {}})
        record["channels"][streamer] = {
            "last_seen": ts,
            "message_count": entry.get("message_count", 0),
        }

    for entry in entries:
        uname = entry.get("username")
        if not uname:
            continue
        channels = _chatter_index.get(uname, {}).get("channels", {})
        entry["cross_channel_count"] = max(0, len(channels) - 1)


def _persist_snapshot(streamer: str, snapshot: dict) -> None:
    try:
        _atomic_write_json(_snapshot_path(streamer), snapshot)
    except Exception:
        logger.warning("chat_activity: failed to persist snapshot for %s", streamer, exc_info=True)


def _persist_index() -> None:
    try:
        _atomic_write_json(_index_path(), _chatter_index)
    except Exception:
        logger.warning("chat_activity: failed to persist chatter index", exc_info=True)


async def start_aggregator() -> None:
    """Long-lived consumer over the whole topic. Intended to be run as one
    background task from the app's lifespan, guarded by the streamers module
    flag same as the streamers router itself."""
    global _aggregator_task
    _load_state()
    _aggregator_task = asyncio.current_task()
    consumer = AIOKafkaConsumer(
        settings.TOPIC_CHAT_ACTIVITY,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                snapshot = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue
            streamer = snapshot.get("login")
            if not streamer:
                continue
            ts = (msg.timestamp or 0) / 1000.0
            _update_chatter_index(streamer, snapshot, ts)
            _latest[streamer] = snapshot
            _history.setdefault(streamer, deque(maxlen=_HISTORY_LEN)).append(snapshot)
            _persist_snapshot(streamer, snapshot)
            _persist_index()
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


async def stop_aggregator() -> None:
    global _aggregator_task
    task = _aggregator_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _aggregator_task = None


def get_snapshot(streamer: str) -> dict | None:
    """Current aggregate snapshot + short recent history for one streamer, or
    None if nothing's been recorded for it yet (never watchlisted, or the
    poller hasn't run a cycle)."""
    if streamer not in _latest:
        return None
    return {**_latest[streamer], "history": list(_history.get(streamer, []))}


async def tail_streamer(streamer: str) -> AsyncIterator[dict]:
    """Per-connection live tail for one streamer -- mirrors services/kafka.py's
    tail(), filtered server-side so SSE clients don't have to."""
    consumer = AIOKafkaConsumer(
        settings.TOPIC_CHAT_ACTIVITY,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                snapshot = json.loads(msg.value.decode("utf-8"))
            except Exception:
                continue
            if snapshot.get("login") != streamer:
                continue
            yield snapshot
            await asyncio.sleep(0)
    finally:
        await consumer.stop()

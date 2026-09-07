"""Overlay chat relay: an anonymous, target-swappable chat reader fanned out to
SSE subscribers and Kafka ``overlay_chat_relay``.

Feeds the left-side colorful chat column in
``overlays/tunastarlink/overlay.html`` (DesktopShare). The idle target is
@tunastarlink's own Twitch chat; ``POST /api/overlay/relay {"channel": ...}``
repoints it at another streamer for raids/collabs.

Two platforms (v2, #300):
- **Twitch** — an anonymous ``justinfan`` IRC read socket (the handshake reused
  from ``inspector._capture_twitch_chat_sync``), made persistent.
- **Kick** — an anonymous Pusher WebSocket on ``chatrooms.{id}.v2`` (the
  mechanism reused from ``inspector._capture_kick_chat``), made persistent.
  Target it with ``kick:<slug>`` or the ``k:<slug>`` short form.

Both normalize to the overlay's message shape
``{user, color, badges[], text, ts, channel}`` and fan out identically. Flood
handling lives in the overlay JS, not here — the relay is a thin, always-on
pump that reconnects on drop and on every target change.

``off`` / ``me`` / ``own`` / null → @tunastarlink's own chat.
"""
import asyncio
import json
import logging
import time

from config import settings

logger = logging.getLogger(__name__)

_IRC_HOST = "irc.chat.twitch.tv"
_IRC_PORT = 6667

# Kick's public Pusher app (same key its own web client uses; anonymous read of
# a public channel needs no auth) — kept in sync with inspector._PUSHER_URL.
_PUSHER_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=cso-operator-app&version=1.0&flash=false"
)
_KICK_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Poll cadence for noticing a target swap on a quiet channel (both platforms
# wrap their read in this timeout, then re-check the target).
_READ_TIMEOUT = 1.0

_SUBSCRIBER_QUEUE_MAX = 500
_BACKOFF_START = 1.0
_BACKOFF_MAX = 30.0

_own_channel = (settings.OVERLAY_OWN_CHANNEL or "tunastarlink").lstrip("#@").lower()

_target: str = _own_channel
_subscribers: "set[asyncio.Queue]" = set()
_relay_task: "asyncio.Task | None" = None
_producer = None  # AIOKafkaProducer, imported lazily in start_relay
_stop = asyncio.Event()
_target_changed = asyncio.Event()


# ── pure helpers (unit-tested, no network) ──────────────────────────────────

def normalize_target(channel: "str | None") -> str:
    """Map a relay command's channel argument to a target token.

    None / "" / "me" / "off" / "own" / the own-channel → own Twitch chat (bare
    login). A ``kick:<slug>`` or ``k:<slug>`` argument → ``kick:<slug>``. A
    ``kick:``/``k:`` with an empty slug → own chat. Anything else → a bare
    Twitch login (leading ``#``/``@`` stripped, lowercased).
    """
    if not channel:
        return _own_channel
    c = channel.strip().lstrip("#@").lower()
    if c in ("", "me", "off", "own", _own_channel):
        return _own_channel
    if c.startswith("kick:") or c.startswith("k:"):
        slug = c.split(":", 1)[1].strip().lstrip("#@")
        return f"kick:{slug}" if slug else _own_channel
    return c


def is_kick(target: str) -> bool:
    return target.startswith("kick:")


def parse_privmsg(line: str) -> "dict | None":
    """Parse one raw Twitch IRC line into the overlay message shape, or None if
    it isn't a chat PRIVMSG. ``channel`` is the source login (from the PRIVMSG
    target), so a mid-swap line is still labeled with its real channel."""
    if "PRIVMSG" not in line:
        return None
    tags: "dict[str, str]" = {}
    rest = line
    if line.startswith("@"):
        tag_part, _, rest = line.partition(" ")
        for kv in tag_part[1:].split(";"):
            k, _, v = kv.partition("=")
            tags[k] = v
    prefix, sep, msg_rest = rest.partition(" PRIVMSG ")
    if not sep:
        return None
    nick = prefix.split("!", 1)[0].lstrip(":")
    chan_part, _, content = msg_rest.partition(" :")
    if not content and ":" in msg_rest:
        chan_part, _, content = msg_rest.partition(":")
    channel = chan_part.strip().lstrip("#").lower()
    text = content.strip()
    if not text:
        return None
    badges = [b.split("/", 1)[0] for b in tags.get("badges", "").split(",") if b]
    return {
        "user": tags.get("display-name") or nick,
        "color": tags.get("color", "") or "",
        "badges": badges,
        "text": text,
        "ts": time.time(),
        "channel": channel or _target,
    }


def parse_kick_event(raw: str, slug: str) -> "dict | None":
    """Parse one Kick Pusher frame into the overlay message shape, or None if it
    isn't a chat message. ``channel`` is ``kick:<slug>`` so the overlay HUD reads
    it as a relayed (non-own) channel and never collides with a Twitch login."""
    try:
        outer = json.loads(raw)
    except Exception:
        return None
    if outer.get("event") != "App\\Events\\ChatMessageEvent":
        return None
    try:
        data = json.loads(outer["data"])
    except Exception:
        return None
    sender = data.get("sender", {}) or {}
    identity = sender.get("identity", {}) or {}
    text = (data.get("content") or "").strip()
    if not text:
        return None
    badges = [b.get("type") for b in identity.get("badges", []) if b.get("type")]
    return {
        "user": sender.get("username") or "anon",
        "color": identity.get("color", "") or "",
        "badges": badges,
        "text": text,
        "ts": time.time(),
        "channel": f"kick:{slug}",
    }


# ── subscriber registry (SSE fan-out) ───────────────────────────────────────

def subscribe() -> "asyncio.Queue":
    q: "asyncio.Queue" = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_MAX)
    _subscribers.add(q)
    return q


def unsubscribe(q: "asyncio.Queue") -> None:
    _subscribers.discard(q)


def current_target() -> str:
    return _target


def _fanout(msg: dict) -> None:
    for q in list(_subscribers):
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.put_nowait(msg)
            except Exception:
                pass


async def _publish_kafka(msg: dict) -> None:
    if _producer is None:
        return
    try:
        await _producer.send(settings.OVERLAY_RELAY_TOPIC, json.dumps(msg).encode("utf-8"))
    except Exception:
        logger.debug("overlay_relay: kafka publish failed", exc_info=True)


def _deliver(msg: "dict | None") -> None:
    if msg is None:
        return
    _fanout(msg)
    # Fire the Kafka publish without blocking the read loop on it.
    asyncio.get_event_loop().create_task(_publish_kafka(msg))


# ── target swap ─────────────────────────────────────────────────────────────

async def set_target(channel: "str | None") -> str:
    """Repoint the relay. The running connection notices ``_target`` changed
    (within ~1s) or is woken by ``_target_changed`` and reconnects to the new
    target — Twitch or Kick. Returns the resolved target token."""
    global _target
    new = normalize_target(channel)
    if new != _target:
        logger.info("overlay_relay: target %s -> %s", _target, new)
        _target = new
        _target_changed.set()
    return new


# ── Twitch IRC connection ───────────────────────────────────────────────────

async def _run_twitch(target: str) -> None:
    """Read @<target>'s Twitch chat until the target changes or the socket drops."""
    reader, writer = await asyncio.open_connection(_IRC_HOST, _IRC_PORT)
    anon_nick = f"justinfan{int(time.time()) % 100000}"

    async def send(msg: str) -> None:
        writer.write((msg + "\r\n").encode("utf-8"))
        await writer.drain()

    try:
        await send("CAP REQ :twitch.tv/tags twitch.tv/commands")
        await send(f"NICK {anon_nick}")
        await send(f"JOIN #{target}")
        logger.info("overlay_relay: twitch connected, joined #%s", target)
        while not _stop.is_set() and _target == target:
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)
            except asyncio.TimeoutError:
                continue  # quiet channel — loop back to re-check _target
            if not raw:
                break  # EOF
            line = raw.decode("utf-8", errors="ignore").rstrip("\r\n")
            if not line:
                continue
            if line.startswith("PING"):
                await send(line.replace("PING", "PONG", 1))
                continue
            _deliver(parse_privmsg(line))
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# ── Kick Pusher connection ──────────────────────────────────────────────────

async def _kick_chatroom_id(slug: str) -> "int | None":
    import httpx

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(
            f"https://kick.com/api/v2/channels/{slug}/chatroom",
            headers=_KICK_BROWSER_HEADERS,
        )
    if r.status_code != 200:
        return None
    return r.json().get("id")


async def _run_kick(target: str) -> None:
    """Read a Kick channel's chat over Pusher until the target changes or drops."""
    import websockets

    slug = target[len("kick:"):]
    chatroom_id = await _kick_chatroom_id(slug)
    if chatroom_id is None:
        logger.warning("overlay_relay: kick slug %r has no chatroom; holding", slug)
        # Nothing to read — wait for a target change rather than hot-looping.
        try:
            await asyncio.wait_for(_target_changed.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass
        return

    async with websockets.connect(_PUSHER_URL, open_timeout=10) as ws:
        await ws.recv()  # pusher:connection_established
        await ws.send(json.dumps({
            "event": "pusher:subscribe",
            "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
        }))
        logger.info("overlay_relay: kick connected, subscribed chatrooms.%s.v2 (%s)", chatroom_id, slug)
        while not _stop.is_set() and _target == target:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_READ_TIMEOUT)
            except asyncio.TimeoutError:
                continue
            try:
                evt = json.loads(raw).get("event")
            except Exception:
                evt = None
            if evt == "pusher:ping":
                # App-level keepalive — Pusher drops us if we never pong.
                await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
                continue
            _deliver(parse_kick_event(raw, slug))


# ── the relay task ──────────────────────────────────────────────────────────

async def start_relay() -> None:
    """Long-lived task (run from the app lifespan under the streamers module):
    hold one Kafka producer open and keep a chat connection to the current
    target alive — Twitch IRC or Kick Pusher — reconnecting on drop and on every
    target change, until stop_relay() cancels it."""
    global _relay_task, _producer
    from aiokafka import AIOKafkaProducer

    _relay_task = asyncio.current_task()
    _stop.clear()
    _producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    try:
        await _producer.start()
    except Exception:
        logger.warning("overlay_relay: kafka producer failed to start; SSE only", exc_info=True)
        _producer = None

    backoff = _BACKOFF_START
    try:
        while not _stop.is_set():
            target = _target
            _target_changed.clear()
            try:
                if is_kick(target):
                    await _run_kick(target)
                else:
                    await _run_twitch(target)
                backoff = _BACKOFF_START  # clean return (target change or EOF) — retry promptly
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("overlay_relay: connection error (%s); retry in %.0fs",
                               target, backoff, exc_info=True)
                try:
                    await asyncio.wait_for(_stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, _BACKOFF_MAX)
    except asyncio.CancelledError:
        pass
    finally:
        if _producer is not None:
            try:
                await _producer.stop()
            except Exception:
                pass
            _producer = None


async def stop_relay() -> None:
    global _relay_task
    _stop.set()
    _target_changed.set()
    task = _relay_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _relay_task = None

"""Overlay chat relay: one anonymous Twitch IRC read socket, target-swappable,
fanned out to SSE subscribers and Kafka ``overlay_chat_relay``.

Feeds the left-side colorful chat column in
``overlays/tunastarlink/overlay.html`` (DesktopShare). The idle target is
@tunastarlink's own chat; ``POST /api/overlay/relay {"channel": ...}`` repoints
it at another streamer for raids/collabs, and ``channel`` null / ``"me"`` /
``"off"`` / ``"own"`` returns it to own chat.

Reuses the proven anonymous ``justinfan`` IRC handshake from
``inspector._capture_twitch_chat_sync`` (CAP REQ tags, NICK, JOIN, PING/PONG,
IRCv3 tag parse). The difference: that one is a bounded, blocking, thread-run
capture; this stays connected as a persistent asyncio socket and swaps channel
live on command.

Flood handling lives in the overlay JS, not here (see the plan,
``streamers/twitch-overlay-chat-relay-plan.md``) — the relay emits every
PRIVMSG; the browser queues, drains at a fixed rate, dedups copypasta, and
shows a live msg/s badge. So the relay stays a thin, always-on pump: parse →
Kafka + SSE fan-out.

v1 is Twitch-only (Kick relay is out of scope); a ``kick:`` target falls back
to own chat rather than erroring.
"""
import asyncio
import json
import logging
import time

from config import settings

logger = logging.getLogger(__name__)

_IRC_HOST = "irc.chat.twitch.tv"
_IRC_PORT = 6667

# Per-subscriber SSE queue bound. At flood scale the overlay drains far slower
# than chat arrives; a stalled/slow client must not grow memory unboundedly, so
# an over-full queue drops its oldest (the overlay is showing a trickle anyway).
_SUBSCRIBER_QUEUE_MAX = 500

# Reconnect backoff (seconds) — capped so a persistent Twitch-side outage keeps
# retrying about every 30s rather than backing off forever.
_BACKOFF_START = 1.0
_BACKOFF_MAX = 30.0

_own_channel = (settings.OVERLAY_OWN_CHANNEL or "tunastarlink").lstrip("#@").lower()

_target: str = _own_channel
_subscribers: "set[asyncio.Queue]" = set()
_relay_task: "asyncio.Task | None" = None
# AIOKafkaProducer, imported lazily in start_relay so the pure parse/normalize
# helpers (and their tests) don't require the Kafka client.
_producer = None
_writer: "asyncio.StreamWriter | None" = None
_write_lock = asyncio.Lock()
_stop = asyncio.Event()


# ── pure helpers (unit-tested, no network) ──────────────────────────────────

def normalize_target(channel: "str | None") -> str:
    """Map a relay command's channel argument to the Twitch login to join.

    None / "" / "me" / "off" / "own" / the own-channel itself → own chat.
    A ``kick:slug`` target falls back to own chat (v1 is Twitch-only). Anything
    else is treated as a bare Twitch login: leading ``#``/``@`` stripped,
    lowercased.
    """
    if not channel:
        return _own_channel
    c = channel.strip().lstrip("#@").lower()
    if c in ("", "me", "off", "own", _own_channel):
        return _own_channel
    if c.startswith("kick:"):
        return _own_channel
    return c


def parse_privmsg(line: str) -> "dict | None":
    """Parse one raw IRC line into the overlay message shape, or None if it
    isn't a chat PRIVMSG.

    Shape (matches overlay.html's SSE contract and the plan):
        {"user", "color", "badges": [type...], "text", "ts", "channel"}

    ``color`` is the chatter's IRCv3 color tag ("" when they have none — the
    overlay then assigns a stable hash color). ``badges`` are bare type names
    ("subscriber", "moderator", …) with the "/count" suffix dropped, which is
    exactly what the overlay's glyph map keys on. ``channel`` is the login the
    line came from (parsed from the PRIVMSG target, so a mid-swap line is still
    labeled with its real source channel).
    """
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
        # Defensive: some servers use "#chan :text" without the leading space
        # we split on above; fall back to the first ':'.
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
    """Deliver one message to every SSE subscriber, dropping the oldest on a
    full (slow-client) queue so a stalled browser can't grow memory."""
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
        # send() (not send_and_wait) just buffers into the producer's batch —
        # non-blocking on the read loop even under flood. Best-effort: the
        # overlay is fed by SSE regardless, so a Kafka hiccup never stalls chat.
        await _producer.send(settings.OVERLAY_RELAY_TOPIC, json.dumps(msg).encode("utf-8"))
    except Exception:
        logger.debug("overlay_relay: kafka publish failed", exc_info=True)


# ── target swap ─────────────────────────────────────────────────────────────

async def set_target(channel: "str | None") -> str:
    """Repoint the relay at ``channel`` (see normalize_target). Sends PART on
    the old channel and JOIN on the new over the live socket when connected;
    if the socket is down, just records the target and the reconnect loop joins
    it. Returns the resolved login now being relayed."""
    global _target
    new = normalize_target(channel)
    old = _target
    _target = new
    if new == old:
        return new
    async with _write_lock:
        w = _writer
        if w is not None:
            try:
                if old:
                    w.write(f"PART #{old}\r\n".encode("utf-8"))
                w.write(f"JOIN #{new}\r\n".encode("utf-8"))
                await w.drain()
            except Exception:
                logger.warning("overlay_relay: swap write failed; reconnect will rejoin", exc_info=True)
    logger.info("overlay_relay: target %s -> %s", old, new)
    return new


# ── the relay task ──────────────────────────────────────────────────────────

async def _run_connection() -> None:
    """One IRC connection lifetime: connect, join the current target, pump
    PRIVMSGs to Kafka + SSE until the socket drops or we're asked to stop."""
    global _writer
    reader, writer = await asyncio.open_connection(_IRC_HOST, _IRC_PORT)
    anon_nick = f"justinfan{int(time.time()) % 100000}"

    async def send(msg: str) -> None:
        writer.write((msg + "\r\n").encode("utf-8"))
        await writer.drain()

    await send("CAP REQ :twitch.tv/tags twitch.tv/commands")
    await send(f"NICK {anon_nick}")
    await send(f"JOIN #{_target}")
    async with _write_lock:
        _writer = writer
    logger.info("overlay_relay: connected, joined #%s as %s", _target, anon_nick)

    try:
        while not _stop.is_set():
            raw = await reader.readline()
            if not raw:  # EOF — server closed the socket
                break
            line = raw.decode("utf-8", errors="ignore").rstrip("\r\n")
            if not line:
                continue
            if line.startswith("PING"):
                await send(line.replace("PING", "PONG", 1))
                continue
            msg = parse_privmsg(line)
            if msg is None:
                continue
            await _publish_kafka(msg)
            _fanout(msg)
    finally:
        async with _write_lock:
            if _writer is writer:
                _writer = None
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def start_relay() -> None:
    """Long-lived task (run from the app lifespan under the streamers module):
    hold one Kafka producer open and keep an anon IRC socket connected, with
    reconnect backoff, until stop_relay() cancels it."""
    global _relay_task, _producer
    from aiokafka import AIOKafkaProducer

    _relay_task = asyncio.current_task()
    _stop.clear()
    _producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    try:
        await _producer.start()
    except Exception:
        logger.warning("overlay_relay: kafka producer failed to start; relaying via SSE only", exc_info=True)
        _producer = None

    backoff = _BACKOFF_START
    try:
        while not _stop.is_set():
            try:
                await _run_connection()
                backoff = _BACKOFF_START  # clean disconnect — retry promptly
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("overlay_relay: connection error; retrying in %.0fs", backoff, exc_info=True)
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
    task = _relay_task
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    _relay_task = None

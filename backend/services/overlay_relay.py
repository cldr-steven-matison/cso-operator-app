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
import re
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


# ── Emotes (#311) ───────────────────────────────────────────────────────────
# The overlay renders ``segments`` — text runs and emote images — resolved here
# so the page needs no CDN knowledge and no position math. ``text`` is left
# exactly as received: the overlay's dedup/collapse keys on it.

_TWITCH_EMOTE_URL = "https://static-cdn.jtvnw.net/emoticons/v2/{id}/default/dark/2.0"
_KICK_EMOTE_URL = "https://files.kick.com/emotes/{id}/fullsize"
_KICK_EMOTE_RE = re.compile(r"\[emote:(\d+):([^\]]*)\]")

# Third-party Twitch emotes (BTTV/FFZ/7TV) never appear in the IRCv3 ``emotes``
# tag, so Twitch's own parse can't see KEKW/OMEGALUL/monkaS/etc. — the bulk of
# what most channels actually spam. We fetch each channel's sets over HTTP (see
# below) into a ``{name: url}`` map and match them here as whole whitespace-
# delimited tokens, after the first-party pass. Kick has no such gap: every Kick
# emote already arrives as an inline ``[emote:id:name]`` token.
_WORD_SPLIT_RE = re.compile(r"(\s+)")


def _twitch_firstparty_segments(text: str, emotes_tag: str) -> list[dict]:
    """Split ``text`` on the IRCv3 ``emotes`` tag (``id:s-e,s-e/id:s-e``).
    Offsets are Unicode code-point indexes into the message, inclusive."""
    spans: list[tuple[int, int, str]] = []
    for part in (emotes_tag or "").split("/"):
        emote_id, _, ranges = part.partition(":")
        if not emote_id or not ranges:
            continue
        for rng in ranges.split(","):
            start, _, end = rng.partition("-")
            if start.isdigit() and end.isdigit():
                spans.append((int(start), int(end), emote_id))
    if not spans:
        return [{"t": "txt", "v": text}] if text else []
    spans.sort()
    segments: list[dict] = []
    pos = 0
    for start, end, emote_id in spans:
        if start < pos or end >= len(text):
            continue  # overlapping or out-of-range tag — keep the text as text
        if start > pos:
            segments.append({"t": "txt", "v": text[pos:start]})
        name = text[start:end + 1]
        segments.append({"t": "em", "id": emote_id, "v": name,
                         "url": _TWITCH_EMOTE_URL.format(id=emote_id)})
        pos = end + 1
    if pos < len(text):
        segments.append({"t": "txt", "v": text[pos:]})
    return segments


def _thirdparty_split(run: str, thirdparty: "dict[str, str]") -> list[dict]:
    """Replace whole whitespace-delimited tokens in ``run`` that name a
    third-party emote with an image segment; keep everything else as text
    runs (whitespace preserved, adjacent text coalesced)."""
    if not run:
        return []
    out: list[dict] = []
    buf: list[str] = []

    def flush() -> None:
        if buf:
            out.append({"t": "txt", "v": "".join(buf)})
            buf.clear()

    for tok in _WORD_SPLIT_RE.split(run):
        url = thirdparty.get(tok) if tok and not tok.isspace() else None
        if url:
            flush()
            out.append({"t": "em", "id": "", "v": tok, "url": url})
        else:
            buf.append(tok)
    flush()
    return out


def twitch_segments(text: str, emotes_tag: str,
                    thirdparty: "dict[str, str] | None" = None) -> list[dict]:
    """First-party emotes from the IRCv3 tag, then — inside the remaining text
    runs — third-party (BTTV/FFZ/7TV) emotes from ``thirdparty`` (name→url)."""
    segments = _twitch_firstparty_segments(text, emotes_tag)
    if not thirdparty:
        return segments
    out: list[dict] = []
    for s in segments:
        if s["t"] == "txt":
            out.extend(_thirdparty_split(s["v"], thirdparty))
        else:
            out.append(s)
    return out


def kick_segments(text: str) -> list[dict]:
    """Split Kick chat content on its inline ``[emote:<id>:<name>]`` tokens."""
    segments: list[dict] = []
    pos = 0
    for m in _KICK_EMOTE_RE.finditer(text):
        if m.start() > pos:
            segments.append({"t": "txt", "v": text[pos:m.start()]})
        segments.append({"t": "em", "id": m.group(1), "v": m.group(2) or m.group(1),
                         "url": _KICK_EMOTE_URL.format(id=m.group(1))})
        pos = m.end()
    if pos < len(text):
        segments.append({"t": "txt", "v": text[pos:]})
    return segments


# ── Third-party emote fetch/cache (BTTV / FFZ / 7TV) ─────────────────────────
# Per-channel ``{name: url}`` maps, globals merged in once and channel emotes
# layered on top. Filled by _ensure_thirdparty() when the Twitch target is
# joined; read synchronously by parse_privmsg. Every fetch is best-effort — a
# down service or a channel with no sets just means fewer emotes, never an error.

_BTTV_EMOTE_URL = "https://cdn.betterttv.net/emote/{id}/2x.webp"
_SEVENTV_EMOTE_URL = "https://cdn.7tv.app/emote/{id}/2x.webp"
_THIRDPARTY_TTL = 1800.0  # re-fetch a channel's (and the global) sets at most this often

_thirdparty_maps: "dict[str, dict[str, str]]" = {}   # login -> {name: url}, global+channel
_thirdparty_fetched: "dict[str, float]" = {}
_thirdparty_global: "dict[str, str] | None" = None
_thirdparty_global_fetched: float = 0.0


def _ffz_url(emoticon: dict) -> "str | None":
    urls = emoticon.get("urls") or {}
    u = urls.get("2") or urls.get("1") or urls.get("4")
    if not u:
        return None
    return ("https:" + u) if u.startswith("//") else u


async def _fetch_bttv_global(client) -> "dict[str, str]":
    r = await client.get("https://api.betterttv.net/3/cached/emotes/global", timeout=10.0)
    return {e["code"]: _BTTV_EMOTE_URL.format(id=e["id"]) for e in r.json()} if r.status_code == 200 else {}


async def _fetch_bttv_channel(client, twitch_id) -> "dict[str, str]":
    r = await client.get(f"https://api.betterttv.net/3/cached/users/twitch/{twitch_id}", timeout=10.0)
    if r.status_code != 200:
        return {}
    d = r.json()
    return {e["code"]: _BTTV_EMOTE_URL.format(id=e["id"])
            for e in (d.get("channelEmotes") or []) + (d.get("sharedEmotes") or [])}


async def _fetch_ffz_global(client) -> "dict[str, str]":
    r = await client.get("https://api.frankerfacez.com/v1/set/global", timeout=10.0)
    out: "dict[str, str]" = {}
    if r.status_code == 200:
        for s in (r.json().get("sets") or {}).values():
            for e in s.get("emoticons", []):
                u = _ffz_url(e)
                if u:
                    out[e["name"]] = u
    return out


async def _fetch_ffz_room(client, login) -> "tuple[dict[str, str], int | None]":
    """FFZ's room endpoint is keyed by login and returns the channel's FFZ
    emotes *and* the numeric ``twitch_id`` — our anonymous id source for the
    BTTV/7TV lookups, so most channels need no Twitch auth at all."""
    r = await client.get(f"https://api.frankerfacez.com/v1/room/{login}", timeout=10.0)
    out: "dict[str, str]" = {}
    if r.status_code != 200:
        return out, None
    d = r.json()
    for s in (d.get("sets") or {}).values():
        for e in s.get("emoticons", []):
            u = _ffz_url(e)
            if u:
                out[e["name"]] = u
    return out, (d.get("room") or {}).get("twitch_id")


async def _fetch_7tv_global(client) -> "dict[str, str]":
    r = await client.get("https://7tv.io/v3/emote-sets/global", timeout=10.0)
    return {e["name"]: _SEVENTV_EMOTE_URL.format(id=e["id"])
            for e in (r.json().get("emotes") or [])} if r.status_code == 200 else {}


async def _fetch_7tv_channel(client, twitch_id) -> "dict[str, str]":
    r = await client.get(f"https://7tv.io/v3/users/twitch/{twitch_id}", timeout=10.0)
    if r.status_code != 200:
        return {}
    es = ((r.json().get("emote_set") or {}).get("emotes")) or []
    return {e["name"]: _SEVENTV_EMOTE_URL.format(id=e["id"]) for e in es}


async def _resolve_twitch_id(client, login: str) -> "int | str | None":
    """Fallback when FFZ has no room for the channel: resolve login→id via
    Helix, reusing the streamers module's app token. Silent if creds absent."""
    try:
        from services.streamers import _twitch_token_refresh, _get_broadcaster_id
        token = await _twitch_token_refresh(client)
        return await _get_broadcaster_id(client, token, login)
    except Exception:
        logger.debug("overlay_relay: helix id resolve failed for %s", login, exc_info=True)
        return None


async def _ensure_thirdparty(login: str) -> None:
    """Populate ``_thirdparty_maps[login]`` (TTL-guarded). Called on target
    join; safe to await — it never raises and never blocks the read loop for
    more than the HTTP timeouts."""
    global _thirdparty_global, _thirdparty_global_fetched
    now = time.time()
    if login in _thirdparty_fetched and now - _thirdparty_fetched[login] < _THIRDPARTY_TTL:
        return
    import httpx

    async def _try(coro):
        try:
            return await coro
        except Exception:
            logger.debug("overlay_relay: third-party sub-fetch failed", exc_info=True)
            return {}

    try:
        async with httpx.AsyncClient(headers={"User-Agent": _KICK_BROWSER_HEADERS["User-Agent"]}) as client:
            if _thirdparty_global is None or now - _thirdparty_global_fetched > _THIRDPARTY_TTL:
                g: "dict[str, str]" = {}
                g.update(await _try(_fetch_bttv_global(client)))
                g.update(await _try(_fetch_ffz_global(client)))
                g.update(await _try(_fetch_7tv_global(client)))
                _thirdparty_global = g
                _thirdparty_global_fetched = now
            merged = dict(_thirdparty_global)
            ffz_room, twitch_id = await _fetch_ffz_room(client, login)
            merged.update(ffz_room)
            if not twitch_id:
                twitch_id = await _resolve_twitch_id(client, login)
            if twitch_id:
                merged.update(await _try(_fetch_bttv_channel(client, twitch_id)))
                merged.update(await _try(_fetch_7tv_channel(client, twitch_id)))
            _thirdparty_maps[login] = merged
            _thirdparty_fetched[login] = now
            logger.info("overlay_relay: %d third-party emotes loaded for #%s", len(merged), login)
    except Exception:
        logger.warning("overlay_relay: third-party emote fetch failed for %s", login, exc_info=True)


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
        "segments": twitch_segments(text, tags.get("emotes", ""),
                                    _thirdparty_maps.get(channel or _target)),
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
        "segments": kick_segments(text),
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
        # Load this channel's BTTV/FFZ/7TV emotes so the first messages already
        # render them (TTL-cached, so a reconnect to the same target is instant).
        await _ensure_thirdparty(target)
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

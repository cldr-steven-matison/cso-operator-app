# services/inspector.py
"""Streamer inspector: live status, recent clips (metadata only, no download),
and who's actually in their chat right now — including bot detection — for the
Streamers app's Inspector sub-page. Mostly read-only (live status, clip listing,
chat capture never touch the watchlist or fetch pipeline) except for
queue_specific_clip(), which deliberately feeds one manually-picked clip into
the same new_clips pipeline the automated fetch uses — for a clip outside the
automated 45-90s duration window (e.g. a viral clip that runs long).
"""
import asyncio
import html
import json
import re
import socket
import time
from pathlib import Path

import httpx

from config import settings
from services.streamers import (
    _atomic_write_json,
    _load_seen,
    _save_seen,
    _seen_lock,
    _burn_glitch_intro,
    _burn_platform_overlay,
    _download_clip,
    _download_hls_sync,
    _get_broadcaster_id,
    _get_kick_broadcaster_id,
    _gql_clip_mp4_url,
    _kick_headers,
    _kick_token_refresh,
    _KICK_API_BASE,
    _KICK_BROWSER_HEADERS,
    _parse_watch_entry,
    _probe_video_duration,
    _publish_clips_to_kafka,
    _twitch_headers,
    _twitch_token_refresh,
    get_roster,
    is_streamer_live,
)

# Hard ceiling on messages processed in one capture — a mega-channel (tens of
# thousands of viewers) can produce thousands of chat messages in a 25s window,
# and without a cap this would grow memory/response size unboundedly.
_MAX_MESSAGES = 4000
# Below this length, a repeated message is almost certainly organic emote/hype
# spam ("LUL", "W", "GG") rather than a coordinated copypasta/raid cluster —
# excluding short strings keeps the cluster detector from flagging normal chat.
_CLUSTER_MIN_LEN = 12
_CLUSTER_MIN_SENDERS = 4

# Kick's production Pusher app key/cluster — same one its own web client uses,
# reverse-engineered the same way the third-party Kick chat bots we found
# (BotRix, KickBot) are themselves built. Public channel, no auth needed to read.
_PUSHER_URL = (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=cso-operator-app&version=1.0&flash=false"
)

# Known third-party Twitch chat bots — Twitch has no "verified bot" badge the
# way Kick's moderator+verified combo works, so name-matching is the practical
# signal here.
_KNOWN_TWITCH_BOTS = {
    "nightbot", "streamelements", "streamlabs", "moobot", "wizebot",
    "fossabot", "botisimo", "ankhbot", "deepbot", "coebot",
    "stay_hydrated_bot", "sery_bot",
}

# Same idea, Kick side — known third-party bot account names, checked alongside
# the badge signature below (see _kick_is_bot's docstring for why name alone
# isn't enough either, but the combo needed tightening after a real miss).
_KNOWN_KICK_BOTS = {"botrix", "kickbot", "serybot", "sery_bot"}

# Engagement-ratio bot-likelihood heuristic — adopted from reviewing
# botted.wtf's published methodology (unique_chatters / viewer_count; their
# own default flag rule is <10% engagement at >=100 concurrent viewers).
# A cheap secondary signal on top of the per-account bot flags above — a
# channel can have zero flagged accounts and still read as mostly-lurkers-or-
# view-botted if the ratio is this low at real scale.
_ENGAGEMENT_RATIO_THRESHOLD = 0.10
_ENGAGEMENT_MIN_VIEWERS = 100


# ── Clip listing (metadata only — no download/ffmpeg) ──────────────────────

async def _list_twitch_clips(client: httpx.AsyncClient, login: str, limit: int) -> list[dict]:
    token = await _twitch_token_refresh(client)
    broadcaster_id = await _get_broadcaster_id(client, token, login)
    if broadcaster_id is None:
        return []
    r = await client.get(
        "https://api.twitch.tv/helix/clips",
        params={"broadcaster_id": broadcaster_id, "first": str(limit)},
        headers=_twitch_headers(token),
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    return [
        {
            "clip_id": c.get("id"),
            "title": c.get("title"),
            "duration": c.get("duration"),
            "view_count": c.get("view_count"),
            "created_at": c.get("created_at"),
            "thumbnail_url": c.get("thumbnail_url"),
            "url": c.get("url"),
        }
        for c in r.json().get("data", [])
    ]


async def _list_kick_clips(client: httpx.AsyncClient, slug: str, limit: int) -> list[dict]:
    r = await client.get(
        f"https://kick.com/api/v2/channels/{slug}/clips",
        headers=_KICK_BROWSER_HEADERS,
        timeout=10.0,
    )
    if r.status_code != 200:
        return []
    return [
        {
            "clip_id": c.get("id"),
            "title": html.unescape(c.get("title", "")),
            "duration": c.get("duration"),
            "view_count": c.get("view_count"),
            "created_at": c.get("created_at"),
            "thumbnail_url": c.get("thumbnail_url"),
            "url": c.get("clip_url"),
        }
        for c in r.json().get("clips", [])[:limit]
    ]


# ── Live chat capture ────────────────────────────────────────────────────────

async def _get_kick_chatroom_id(client: httpx.AsyncClient, slug: str) -> int | None:
    r = await client.get(
        f"https://kick.com/api/v2/channels/{slug}/chatroom",
        headers=_KICK_BROWSER_HEADERS,
        timeout=10.0,
    )
    if r.status_code != 200:
        return None
    return r.json().get("id")


def _kick_is_bot(uname: str, badges: list[dict]) -> bool:
    # moderator+verified alone isn't enough — confirmed live 2026-07-26 on
    # trainwreckstv's channel: two real human mods ("Adam", "zoro", both
    # posting normal human chat like "so now what") carried this exact badge
    # pair too, on a channel with real celebrity/notable-figure moderators.
    # BotRix/KickBot (the two confirmed real bots, on bbjess's channel) both
    # also had "bot" in the name — require that too, or a known-name match.
    types = {b.get("type") for b in badges}
    has_badge_signature = "moderator" in types and "verified" in types
    name_lower = uname.lower()
    return has_badge_signature and (name_lower in _KNOWN_KICK_BOTS or "bot" in name_lower)


async def _capture_kick_chat(chatroom_id: int, duration_sec: int) -> dict:
    import websockets

    chatters: dict[str, dict] = {}
    all_messages: list[tuple[str, str]] = []
    messages_seen = 0
    cap_hit = False
    try:
        async with websockets.connect(_PUSHER_URL, open_timeout=10) as ws:
            await ws.recv()  # connection_established
            await ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }))
            end = time.time() + duration_sec
            while time.time() < end:
                if messages_seen >= _MAX_MESSAGES:
                    cap_hit = True
                    break
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.5, end - time.time()))
                except asyncio.TimeoutError:
                    break
                try:
                    outer = json.loads(raw)
                except Exception:
                    continue
                if outer.get("event") != "App\\Events\\ChatMessageEvent":
                    continue
                data = json.loads(outer["data"])
                sender = data.get("sender", {})
                uname = sender.get("username", "?")
                # Full badge dicts kept (type + count, e.g. subscriber months) —
                # decoded into display labels later in inspect_chat().
                badges = sender.get("identity", {}).get("badges", [])
                content = data.get("content", "")
                messages_seen += 1
                all_messages.append((uname, content))
                entry = chatters.setdefault(uname, {
                    "username": uname, "message_count": 0,
                    "raw_badges": badges, "samples": [], "is_bot": _kick_is_bot(uname, badges),
                })
                entry["message_count"] += 1
                if len(entry["samples"]) < 3:
                    entry["samples"].append(content)
    except Exception as e:
        return {"error": str(e), "chatters": list(chatters.values()), "all_messages": all_messages,
                "messages_seen": messages_seen, "cap_hit": cap_hit}
    return {"chatters": list(chatters.values()), "all_messages": all_messages,
            "messages_seen": messages_seen, "cap_hit": cap_hit}


def _capture_twitch_chat_sync(login: str, duration_sec: int) -> dict:
    """Anonymous read-only IRC join (the 'justinfan' trick) — no OAuth needed
    to read a public Twitch chat. Runs in a thread since it's blocking sockets."""
    chatters: dict[str, dict] = {}
    all_messages: list[tuple[str, str]] = []
    messages_seen = 0
    cap_hit = False
    sock = None
    try:
        sock = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=10)
        anon_nick = f"justinfan{int(time.time()) % 100000}"

        def send(msg: str) -> None:
            sock.sendall((msg + "\r\n").encode("utf-8"))

        send("CAP REQ :twitch.tv/tags twitch.tv/commands")
        send(f"NICK {anon_nick}")
        send(f"JOIN #{login.lower()}")

        buffer = ""
        end = time.time() + duration_sec
        while time.time() < end:
            if messages_seen >= _MAX_MESSAGES:
                cap_hit = True
                break
            sock.settimeout(max(0.5, end - time.time()))
            try:
                data = sock.recv(4096).decode("utf-8", errors="ignore")
            except socket.timeout:
                break
            if not data:
                break
            buffer += data
            while "\r\n" in buffer:
                line, buffer = buffer.split("\r\n", 1)
                if line.startswith("PING"):
                    send(line.replace("PING", "PONG", 1))
                    continue
                if "PRIVMSG" not in line:
                    continue
                tags: dict[str, str] = {}
                rest = line
                if line.startswith("@"):
                    tag_part, _, rest = line.partition(" ")
                    for kv in tag_part[1:].split(";"):
                        k, _, v = kv.partition("=")
                        tags[k] = v
                prefix, sep, msg_rest = rest.partition(" PRIVMSG ")
                if not sep:
                    continue
                nick = prefix.split("!", 1)[0].lstrip(":")
                _, _, content = msg_rest.partition(":")
                content = content.strip()
                # Raw "type/count" strings kept (e.g. "subscriber/34") — decoded
                # into display labels later in inspect_chat().
                raw_badges = [b for b in tags.get("badges", "").split(",") if b]
                is_bot = nick.lower() in _KNOWN_TWITCH_BOTS or "bot" in nick.lower()
                messages_seen += 1
                all_messages.append((nick, content))
                entry = chatters.setdefault(nick, {
                    "username": nick, "message_count": 0, "raw_badges": raw_badges,
                    "samples": [], "is_bot": is_bot,
                })
                entry["message_count"] += 1
                if len(entry["samples"]) < 3:
                    entry["samples"].append(content)
    except Exception as e:
        return {"error": str(e), "chatters": list(chatters.values()), "all_messages": all_messages,
                "messages_seen": messages_seen, "cap_hit": cap_hit}
    finally:
        if sock is not None:
            sock.close()
    return {"chatters": list(chatters.values()), "all_messages": all_messages,
            "messages_seen": messages_seen, "cap_hit": cap_hit}


# ── Badge decoding + duplicate-message clustering ───────────────────────────

_KICK_BADGE_LABELS = {
    "moderator": "Moderator", "vip": "VIP", "og": "OG",
    "verified": "Verified Channel", "broadcaster": "Broadcaster",
    "sub_gifter": "Sub Gifter", "founder": "Founder",
}


def _decode_kick_badges(raw_badges: list[dict]) -> tuple[list[str], int]:
    """Returns (display labels, a rough 'investment' score) — score is just
    subscriber months plus a flat bonus per staff/loyalty badge, used to rank
    accounts as more/less established, not a real trust score."""
    labels: list[str] = []
    score = 0
    for b in raw_badges:
        t = b.get("type")
        if t == "subscriber":
            months = b.get("count", 0)
            labels.append(f"Sub ({months}mo)" if months else "Subscriber")
            score += months or 1
        else:
            labels.append(_KICK_BADGE_LABELS.get(t, t or "?"))
            score += 5
    return labels, score


def _decode_twitch_badges(raw_badges: list[str]) -> tuple[list[str], int]:
    labels: list[str] = []
    score = 0
    for raw in raw_badges:
        t, _, count_str = raw.partition("/")
        if t == "subscriber":
            months = int(count_str) if count_str.isdigit() else 0
            labels.append(f"Sub ({months}mo)" if months else "Subscriber")
            score += months or 1
        elif t in ("moderator", "vip", "broadcaster", "founder"):
            labels.append(t.capitalize())
            score += 5
        elif t in ("turbo", "premium"):
            labels.append("Prime" if t == "premium" else "Turbo")
            score += 2
        else:
            labels.append(t)
            score += 1
    return labels, score


_KICK_EMOTE_RE = re.compile(r"\[emote:\d+:[^\]]*\]")


def _find_message_clusters(all_messages: list[tuple[str, str]]) -> list[dict]:
    """Groups near-identical messages (normalized: stripped/lowercased) sent by
    several *different* accounts — the actual signature of a copypasta/raid/
    view-bot-farm spam wave, as opposed to normal repeated hype emotes (which
    are excluded via the min-length floor, plus a Kick-specific check below).
    This is a heuristic signal, not a definitive bot/fake verdict — a real raid
    of genuine fans saying the same hype phrase would also show up here.

    Confirmed live 2026-07-26 against n3on's ~30k-viewer chat: without the
    emote check, 10 accounts spamming a single Kick emote (`[emote:37226:kekw]`
    — long enough to clear the length floor on its own) surfaced as a false
    'spam cluster'. Kick emote tokens are stripped before the length check so a
    message that's just one or more emotes back-to-back doesn't count as text."""
    by_content: dict[str, dict[str, int]] = {}
    for uname, content in all_messages:
        norm = " ".join(content.strip().lower().split())
        text_only = _KICK_EMOTE_RE.sub("", norm).strip()
        if len(text_only) < _CLUSTER_MIN_LEN:
            continue
        senders = by_content.setdefault(norm, {})
        senders[uname] = senders.get(uname, 0) + 1

    clusters = []
    for norm, senders in by_content.items():
        if len(senders) >= _CLUSTER_MIN_SENDERS:
            clusters.append({
                "sample_text": norm[:200],
                "distinct_senders": len(senders),
                "total_messages": sum(senders.values()),
                "senders": sorted(senders, key=lambda u: -senders[u])[:10],
            })
    clusters.sort(key=lambda c: -c["distinct_senders"])
    return clusters[:10]


async def _get_viewer_count(client: httpx.AsyncClient, platform: str, login: str) -> int | None:
    try:
        if platform == "kick":
            token = await _kick_token_refresh(client)
            broadcaster_id = await _get_kick_broadcaster_id(client, token, login)
            if broadcaster_id is None:
                return None
            r = await client.get(
                f"{_KICK_API_BASE}/users/livestreams",
                params={"user_id": broadcaster_id},
                headers=_kick_headers(token),
                timeout=10.0,
            )
            data = r.json().get("data") or []
            return data[0].get("viewer_count") if data else None
        else:
            token = await _twitch_token_refresh(client)
            r = await client.get(
                "https://api.twitch.tv/helix/streams",
                params={"user_login": login},
                headers=_twitch_headers(token),
                timeout=10.0,
            )
            data = r.json().get("data") or []
            return data[0].get("viewer_count") if data else None
    except Exception:
        return None


# ── Live-now discovery ───────────────────────────────────────────────────────

async def list_live_now(client: httpx.AsyncClient) -> list[str]:
    """Every roster entry (Twitch + Kick, not just the watch list) that's live
    right now — for the Inspector's 'Live Now' picker."""
    roster = get_roster()
    results = await asyncio.gather(*(is_streamer_live(client, e) for e in roster))
    return [entry for entry, live in zip(roster, results) if live]


# ── Alt-platform check ───────────────────────────────────────────────────────

async def _check_alt_platform(client: httpx.AsyncClient, platform: str, login: str) -> dict:
    """Best-effort check for the same handle on the *other* platform. Real
    finding that motivated this: bbjess (tracked as kick:bbjess) turned out to
    also have a dormant Twitch account with real, higher-view clips sitting
    under the same handle. Cheap existence+live check only — no clip download,
    no chat capture, just enough to tell you whether it's worth a full Inspect."""
    alt_platform = "twitch" if platform == "kick" else "kick"
    try:
        if alt_platform == "twitch":
            token = await _twitch_token_refresh(client)
            broadcaster_id = await _get_broadcaster_id(client, token, login)
            if broadcaster_id is None:
                return {"platform": "twitch", "exists": False}
            live = await is_streamer_live(client, login)
            sample_clips = await _list_twitch_clips(client, login, 3)
            return {"platform": "twitch", "exists": True, "live": live, "sample_clips": sample_clips}
        else:
            token = await _kick_token_refresh(client)
            broadcaster_id = await _get_kick_broadcaster_id(client, token, login)
            if broadcaster_id is None:
                return {"platform": "kick", "exists": False}
            live = await is_streamer_live(client, f"kick:{login}")
            sample_clips = await _list_kick_clips(client, login, 3)
            return {"platform": "kick", "exists": True, "live": live, "sample_clips": sample_clips}
    except Exception as e:
        return {"platform": alt_platform, "exists": None, "error": str(e)}


# ── Historical inspection (clips + alt-platform, no chat) ───────────────────
# Deliberately fast and chat-free — the Users/Bots page owns live chat capture.

async def inspect_streamer(
    client: httpx.AsyncClient, entry: str, clip_limit: int = 12,
) -> dict:
    platform, login = _parse_watch_entry(entry)
    live = await is_streamer_live(client, entry)

    clips_task = (
        _list_kick_clips(client, login, clip_limit) if platform == "kick"
        else _list_twitch_clips(client, login, clip_limit)
    )
    clips, alt_platform = await asyncio.gather(
        clips_task, _check_alt_platform(client, platform, login),
    )

    return {
        "login": entry,
        "platform": platform,
        "live": live,
        "clips": clips,
        "alt_platform": alt_platform,
    }


# ── Live chat inspection (Users/Bots page) ──────────────────────────────────

_TOP_CHATTERS_LIMIT = 40


async def inspect_chat(
    client: httpx.AsyncClient, entry: str, chat_seconds: int = 25,
) -> dict:
    platform, login = _parse_watch_entry(entry)
    live = await is_streamer_live(client, entry)

    if not live:
        return {
            "login": entry, "platform": platform, "live": False,
            "viewer_count": None, "duration_sec": 0,
            "unique_chatters": 0, "messages_seen": 0, "message_cap_hit": False,
            "bots": [], "chatters": [], "clusters": [],
            "engagement_ratio": None, "bot_flag_likely": False,
            "error": None, "note": "streamer is offline — skipped live chat capture",
        }

    if platform == "kick":
        chatroom_id = await _get_kick_chatroom_id(client, login)
        capture_task = (
            _capture_kick_chat(chatroom_id, chat_seconds) if chatroom_id is not None
            else _no_capture_result("could not resolve Kick chatroom id")
        )
    else:
        capture_task = asyncio.to_thread(_capture_twitch_chat_sync, login, chat_seconds)

    chat_result, viewer_count = await asyncio.gather(
        capture_task, _get_viewer_count(client, platform, login),
    )

    decode = _decode_kick_badges if platform == "kick" else _decode_twitch_badges
    chatters = []
    for c in chat_result.get("chatters", []):
        labels, score = decode(c.get("raw_badges", []))
        chatters.append({
            "username": c["username"], "message_count": c["message_count"],
            "badges": labels, "investment_score": score,
            "samples": c["samples"], "is_bot": c["is_bot"],
        })
    chatters.sort(key=lambda c: -c["message_count"])

    bots = [c for c in chatters if c["is_bot"]]
    humans = [c for c in chatters if not c["is_bot"]][:_TOP_CHATTERS_LIMIT]
    clusters = _find_message_clusters(chat_result.get("all_messages", []))

    # unique_chatters vs viewer_count — low engagement at real scale is a cheap
    # secondary signal on top of the per-account bot flags above (see the
    # threshold constants' docstring near the top of this file).
    engagement_ratio = len(chatters) / viewer_count if viewer_count else None
    bot_flag_likely = (
        engagement_ratio is not None
        and viewer_count >= _ENGAGEMENT_MIN_VIEWERS
        and engagement_ratio < _ENGAGEMENT_RATIO_THRESHOLD
    )

    return {
        "login": entry,
        "platform": platform,
        "live": live,
        "viewer_count": viewer_count,
        "duration_sec": chat_seconds,
        "unique_chatters": len(chatters),
        "messages_seen": chat_result.get("messages_seen", 0),
        "message_cap_hit": chat_result.get("cap_hit", False),
        "bots": bots,
        "chatters": humans,
        "clusters": clusters,
        "engagement_ratio": engagement_ratio,
        "bot_flag_likely": bot_flag_likely,
        "error": chat_result.get("error"),
        "note": chat_result.get("note"),
    }


async def _no_capture_result(error: str) -> dict:
    return {"chatters": [], "all_messages": [], "messages_seen": 0, "cap_hit": False, "error": error}


# ── Queue a specific, manually-picked clip ──────────────────────────────────

async def queue_specific_clip(
    client: httpx.AsyncClient, platform: str, streamer: str, clip_id: str,
    url: str, thumbnail_url: str, title: str, view_count: int, created_at: str,
) -> dict:
    """Download + overlay-burn + publish ONE specific clip straight to the
    new_clips Kafka topic, same as the automated fetch would — except this
    bypasses the 45-90s duration filter entirely, since it's a deliberate,
    manual pick (e.g. a viral clip that happens to run long). Downstream
    (ProcessClips → Whisper/vLLM → approve) is unchanged, same pipeline."""
    clip_dir = Path(settings.CLIP_STORAGE_PATH)
    seen: set[str] = _load_seen()

    full_clip_id = f"kick_{clip_id.replace('-', '')}" if platform == "kick" else clip_id
    if full_clip_id in seen:
        return {"ok": False, "error": "already queued/seen before"}

    dest = clip_dir / f"{full_clip_id}.mp4"
    if not dest.exists():
        if platform == "kick":
            ok = await asyncio.to_thread(_download_hls_sync, url, dest)
        else:
            mp4_url = await _gql_clip_mp4_url(client, clip_id)
            if not mp4_url:
                return {"ok": False, "error": "no download URL resolved"}
            ok = await _download_clip(client, mp4_url, dest)
        if not ok:
            return {"ok": False, "error": "download failed"}
        bar_h = await asyncio.to_thread(_burn_platform_overlay, dest, platform, streamer)
        await asyncio.to_thread(_burn_glitch_intro, dest, bar_h)

    real_duration = await asyncio.to_thread(_probe_video_duration, dest)
    clip = {
        "clip_id": full_clip_id,
        "source": platform,
        "streamer": streamer,
        "title": html.unescape(title),
        "url": url,
        "thumbnail_url": thumbnail_url,
        "duration": real_duration if real_duration is not None else 0,
        "created_at": created_at,
        "clip_path": str(dest),
        "view_count": view_count,
    }
    await _publish_clips_to_kafka([clip])
    # Union under the lock rather than writing back the set we read minutes
    # ago — a chat-trigger or cron fetch running concurrently would otherwise
    # have its own additions silently dropped (#174).
    with _seen_lock():
        _save_seen(_load_seen() | {full_clip_id})

    return {"ok": True, "clip_id": full_clip_id, "duration": clip["duration"]}

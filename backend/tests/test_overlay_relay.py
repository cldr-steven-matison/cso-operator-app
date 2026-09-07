"""Offline unit tests for the overlay chat relay's pure parsing/target logic
(#300). No network, no Kafka — run with `python -m pytest backend/tests` or
directly `python backend/tests/test_overlay_relay.py`."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services import overlay_relay as R  # noqa: E402

OWN = R._own_channel  # "tunastarlink" by default


def test_normalize_target_own_and_aliases():
    for arg in (None, "", "me", "off", "own", OWN, "#" + OWN, "@" + OWN, OWN.upper()):
        assert R.normalize_target(arg) == OWN, arg


def test_normalize_target_other_channel():
    assert R.normalize_target("xQc") == "xqc"
    assert R.normalize_target("#SomeStreamer") == "somestreamer"
    assert R.normalize_target("@dishwasher ") == "dishwasher"


def test_normalize_target_kick():
    # v2: kick: and the k: short form are real targets (not a fallback to own).
    assert R.normalize_target("kick:trainwreck") == "kick:trainwreck"
    assert R.normalize_target("k:Roshtein") == "kick:roshtein"
    assert R.normalize_target("KICK:#BBJess ") == "kick:bbjess"
    assert R.is_kick("kick:trainwreck") is True
    assert R.is_kick("xqc") is False
    # an empty slug is not a channel — fall back to own chat.
    assert R.normalize_target("kick:") == OWN
    assert R.normalize_target("k:") == OWN


def test_parse_kick_event():
    import json
    inner = json.dumps({
        "content": "KEKW that clip",
        "sender": {"username": "Roshtein_fan",
                   "identity": {"color": "#E9113C",
                                "badges": [{"type": "subscriber", "count": 5},
                                           {"type": "moderator"}]}},
    })
    frame = json.dumps({"event": "App\\Events\\ChatMessageEvent", "data": inner})
    msg = R.parse_kick_event(frame, "roshtein")
    assert msg["user"] == "Roshtein_fan"
    assert msg["color"] == "#E9113C"
    assert msg["badges"] == ["subscriber", "moderator"]
    assert msg["text"] == "KEKW that clip"
    assert msg["channel"] == "kick:roshtein"


def test_parse_kick_event_ignores_non_chat():
    import json
    for frame in (
        json.dumps({"event": "pusher:ping", "data": {}}),
        json.dumps({"event": "App\\Events\\SubscriptionEvent", "data": "{}"}),
        "not json",
    ):
        assert R.parse_kick_event(frame, "roshtein") is None


def test_parse_privmsg_full_tags():
    line = (
        "@badges=subscriber/12,moderator/1;color=#1E90FF;display-name=CoolViewer "
        ":coolviewer!coolviewer@coolviewer.tmi.twitch.tv PRIVMSG #xqc :GG that was clean"
    )
    msg = R.parse_privmsg(line)
    assert msg is not None
    assert msg["user"] == "CoolViewer"
    assert msg["color"] == "#1E90FF"
    assert msg["badges"] == ["subscriber", "moderator"]
    assert msg["text"] == "GG that was clean"
    assert msg["channel"] == "xqc"
    assert isinstance(msg["ts"], float)


def test_parse_privmsg_no_color_no_badges():
    line = ":anon!anon@anon.tmi.twitch.tv PRIVMSG #tunastarlink :hello"
    msg = R.parse_privmsg(line)
    assert msg is not None
    assert msg["user"] == "anon"       # falls back to nick when no display-name
    assert msg["color"] == ""          # overlay assigns a hash color
    assert msg["badges"] == []
    assert msg["channel"] == "tunastarlink"


def test_parse_privmsg_text_with_colons():
    line = "@display-name=Nerd :nerd!nerd@nerd.tmi.twitch.tv PRIVMSG #chan :ratio: 3:1 lol"
    msg = R.parse_privmsg(line)
    assert msg is not None
    assert msg["text"] == "ratio: 3:1 lol"


def test_parse_privmsg_non_chat_lines():
    for line in (
        "PING :tmi.twitch.tv",
        ":tmi.twitch.tv 001 justinfan123 :Welcome, GLHF!",
        ":justinfan123.tmi.twitch.tv JOIN #xqc",
        "@msg-id=sub :tmi.twitch.tv USERNOTICE #xqc",
    ):
        assert R.parse_privmsg(line) is None, line


def test_parse_privmsg_empty_text_dropped():
    line = ":anon!anon@anon.tmi.twitch.tv PRIVMSG #chan :   "
    assert R.parse_privmsg(line) is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print("ok", fn.__name__)
    print(f"\n{len(fns)} passed")


# ── Emotes (#311) ───────────────────────────────────────────────────────────

def test_twitch_segments_from_emotes_tag():
    # "Kappa" at 0-4 and 12-16, "LUL" (id 425618) at 6-8; offsets are code points.
    text = "Kappa LUL x Kappa"
    segs = R.twitch_segments(text, "25:0-4,12-16/425618:6-8")
    assert [s["t"] for s in segs] == ["em", "txt", "em", "txt", "em"]
    assert segs[0] == {"t": "em", "id": "25", "v": "Kappa",
                       "url": "https://static-cdn.jtvnw.net/emoticons/v2/25/default/dark/2.0"}
    assert segs[1] == {"t": "txt", "v": " "}
    assert segs[2]["id"] == "425618" and segs[2]["v"] == "LUL"
    assert segs[3] == {"t": "txt", "v": " x "}
    assert segs[4]["v"] == "Kappa"
    assert "".join(s["v"] for s in segs) == text


def test_twitch_segments_code_point_offsets_past_an_emoji():
    # Twitch counts code points: the astral 🐟 is ONE position, not two UTF-16 units.
    text = "🐟 Kappa"
    segs = R.twitch_segments(text, "25:2-6")
    assert segs == [{"t": "txt", "v": "🐟 "},
                    {"t": "em", "id": "25", "v": "Kappa",
                     "url": "https://static-cdn.jtvnw.net/emoticons/v2/25/default/dark/2.0"}]


def test_twitch_segments_no_tag_and_bad_tag():
    assert R.twitch_segments("plain words", "") == [{"t": "txt", "v": "plain words"}]
    # out-of-range / garbage ranges never eat text
    assert R.twitch_segments("hi", "25:0-40") == [{"t": "txt", "v": "hi"}]
    assert R.twitch_segments("hi", "garbage") == [{"t": "txt", "v": "hi"}]


def test_kick_segments_inline_tokens():
    text = "gg [emote:37221:KEKW] wow [emote:1730752:catJAM]"
    segs = R.kick_segments(text)
    assert [s["t"] for s in segs] == ["txt", "em", "txt", "em"]
    assert segs[1] == {"t": "em", "id": "37221", "v": "KEKW",
                       "url": "https://files.kick.com/emotes/37221/fullsize"}
    assert segs[3]["v"] == "catJAM"
    assert R.kick_segments("no emotes here") == [{"t": "txt", "v": "no emotes here"}]


def test_parse_privmsg_carries_segments_and_untouched_text():
    line = ("@badges=;color=;display-name=V;emotes=25:3-7 "
            ":v!v@v.tmi.twitch.tv PRIVMSG #xqc :gg Kappa")
    msg = R.parse_privmsg(line)
    assert msg["text"] == "gg Kappa"                      # dedup key unchanged
    assert msg["segments"][1]["t"] == "em" and msg["segments"][1]["v"] == "Kappa"


def test_parse_kick_event_carries_segments():
    import json
    inner = {"content": "[emote:37221:KEKW] lol",
             "sender": {"username": "k", "identity": {"badges": []}}}
    frame = json.dumps({"event": "App\\Events\\ChatMessageEvent", "data": json.dumps(inner)})
    msg = R.parse_kick_event(frame, "bbjess")
    assert msg["text"] == "[emote:37221:KEKW] lol"
    assert msg["segments"][0]["t"] == "em" and msg["segments"][1] == {"t": "txt", "v": " lol"}

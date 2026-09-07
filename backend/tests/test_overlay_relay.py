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

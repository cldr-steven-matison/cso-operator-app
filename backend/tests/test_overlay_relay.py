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


def test_normalize_target_kick_falls_back_to_own():
    # v1 is Twitch-only; a kick: target must not try to JOIN a bogus IRC channel.
    assert R.normalize_target("kick:trainwreck") == OWN


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

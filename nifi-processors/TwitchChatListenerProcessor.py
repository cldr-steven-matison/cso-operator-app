# TwitchChatListenerProcessor.py
import json
import queue
import socket
import threading
import time
import urllib.request
import urllib.parse

from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators


class TwitchChatListenerProcessor(FlowFileSource):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileSource']

    class ProcessorDetails:
        version = '0.0.21-SNAPSHOT'
        description = 'Holds a persistent connection to Twitch IRC chat and emits one FlowFile per detected "!load <streamer> [screen]" command (screen optional, defaults to screen1, "!l" accepted as a short alias) or "!matrix <screen1|screen2|screen3|screen4>" command (screen required, no default - unlike !load, a bare "!matrix" with no screen is not a recognized command; screen1 targets the Jetson, screen2 targets GamingPC, screen3/screen4 target TunaStarlink). Requests the twitch.tv/tags IRCv3 capability to read each message'"'"'s badges/mod tags. Mod-only short forms: "!m" for !matrix, "k:" in place of "kick:" on a streamer login, and "s1"/"s2"/"s3"/"s4" in place of screen1-4 - each is checked independently, and a non-broadcaster/non-moderator sender using any of them has the whole command silently ignored (same as an unrecognized command); the existing full-text forms (including the pre-existing "!l" alias) keep working for everyone, unchanged. Before dispatching a !load, checks the streamer'"'"'s live status via the Live Check API URL (cso-operator-app, covers both Twitch and Kick "kick:" logins) and replies "not live" instead of queuing if they'"'"'re offline - a lookup failure fails open (dispatches anyway) rather than silently blocking a real load. A global cooldown (Cooldown Seconds property) shared by both commands protects the edge hardware from chat spam - one warning reply per blocked window, then silent. Announces itself once on join (no auto-posted watchlist — reconnects happen often enough that repeating it every time reads as spam); responds to "!commands"/"!help" and "!watchlist" ("!w" alias accepted) on demand only. Mints a fresh access token from the refresh token before every (re)connect, so it never hits the ~4hr access-token expiry. Reconnects with backoff on disconnect.'
        tags = ['twitch', 'irc', 'chat', 'streamers', 'chat-bot']
        dependencies = []

    BOT_USERNAME = PropertyDescriptor(
        name="Bot Username",
        description="Twitch login name of the bot account (e.g. tunastreettest).",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    CHANNEL = PropertyDescriptor(
        name="Channel",
        description="Twitch channel to join, without a leading #.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    CLIENT_ID = PropertyDescriptor(
        name="Client ID",
        description="Twitch app client ID used to mint fresh access tokens via the refresh token grant.",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    CLIENT_SECRET = PropertyDescriptor(
        name="Client Secret",
        description="Twitch app client secret used to mint fresh access tokens via the refresh token grant.",
        required=True,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    REFRESH_TOKEN = PropertyDescriptor(
        name="Refresh Token",
        description="User refresh token for the bot account (chat:read+chat:edit scopes). Used once at startup to mint the first access token — Twitch rotates the refresh token on every use, so this stored value goes stale after the first refresh; it's only a seed, not the token in ongoing use.",
        required=True,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    COMMAND_PREFIX = PropertyDescriptor(
        name="Command Prefix",
        description="Chat command that triggers a load.",
        required=True,
        default_value="!load",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    MATRIX_COMMAND = PropertyDescriptor(
        name="Matrix Command",
        description="Chat command that turns on the Nano's matrix screensaver.",
        required=True,
        default_value="!matrix",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    WATCHLIST_COMMAND = PropertyDescriptor(
        name="Watchlist Command",
        description="Chat command that posts the active streamer watchlist on demand (same message as the on-join post).",
        required=True,
        default_value="!watchlist",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    WATCHLIST_API_URL = PropertyDescriptor(
        name="Watchlist API URL",
        description="cso-operator-app in-cluster URL for the active streamer watchlist, "
                     "fetched and posted to chat on join.",
        required=True,
        default_value="http://cso-operator-app.default.svc.cluster.local:8090/api/streamers/watchlist",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    LIVE_CHECK_API_URL = PropertyDescriptor(
        name="Live Check API URL",
        description="cso-operator-app in-cluster URL checked before dispatching a !load - "
                     "?login=<streamer> is appended (kick: prefix passed through as-is). "
                     "Expected JSON: {\"live\": true|false}.",
        required=True,
        default_value="http://cso-operator-app.default.svc.cluster.local:8090/api/streamers/live",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    COOLDOWN_SECONDS = PropertyDescriptor(
        name="Cooldown Seconds",
        description="Global cooldown shared by !load and !matrix (one timer, not per-command) - "
                     "protects the edge hardware from chat spam. First blocked attempt within the "
                     "window gets one warning reply, further spam in the same window is silent.",
        required=True,
        default_value="10",
        validators=[StandardValidators.NUMBER_VALIDATOR],
    )
    def __init__(self, **kwargs):
        # 'pass' is the safest initialization in many containerized environments —
        # real state is set up in onScheduled, which is guaranteed to run before create().
        pass

    def getPropertyDescriptors(self):
        return [self.BOT_USERNAME, self.CHANNEL, self.CLIENT_ID, self.CLIENT_SECRET,
                self.REFRESH_TOKEN, self.COMMAND_PREFIX, self.MATRIX_COMMAND,
                self.WATCHLIST_COMMAND, self.WATCHLIST_API_URL, self.LIVE_CHECK_API_URL,
                self.COOLDOWN_SECONDS]

    def onScheduled(self, context):
        username = context.getProperty(self.BOT_USERNAME).getValue()
        channel = context.getProperty(self.CHANNEL).getValue().lstrip('#')
        client_id = context.getProperty(self.CLIENT_ID).getValue()
        client_secret = context.getProperty(self.CLIENT_SECRET).getValue()
        prefix = context.getProperty(self.COMMAND_PREFIX).getValue()
        matrix_command = context.getProperty(self.MATRIX_COMMAND).getValue()
        watchlist_command = context.getProperty(self.WATCHLIST_COMMAND).getValue()
        watchlist_url = context.getProperty(self.WATCHLIST_API_URL).getValue()
        live_check_url = context.getProperty(self.LIVE_CHECK_API_URL).getValue()
        cooldown_seconds = context.getProperty(self.COOLDOWN_SECONDS).asFloat()

        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._command_prefix = prefix
        self._matrix_command = matrix_command
        self._watchlist_command = watchlist_command
        self._watchlist_url = watchlist_url
        self._live_check_url = live_check_url
        self._cooldown_seconds = cooldown_seconds
        self._last_command_time = 0.0
        self._cooldown_warned = False
        # Seeded from the property once; rotates in-memory on every refresh after that.
        self._refresh_token = context.getProperty(self.REFRESH_TOKEN).getValue()

        self._thread = threading.Thread(
            target=self._run_irc_loop,
            args=(username, channel, client_id, client_secret),
            daemon=True,
        )
        self._thread.start()

    def onStopped(self, context):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def create(self, context):
        try:
            item = self._queue.get_nowait()
        except queue.Empty:
            return None

        attributes = {'command': item['command'], 'requested_by': item['requested_by'], 'screen': item['screen']}
        if 'streamer' in item:
            attributes['streamer'] = item['streamer']
        if 'display_screen' in item:
            attributes['display_screen'] = item['display_screen']

        return FlowFileSourceResult(
            relationship='success',
            attributes=attributes,
            contents=json.dumps(item),
        )

    # --- IRC connection handling (background thread) ---

    def _refresh_access_token(self, client_id, client_secret):
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }).encode()
        req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=body, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        # Twitch rotates the refresh token on every use — the old one is now invalid,
        # so this new one is what every subsequent refresh (in this running process) must use.
        self._refresh_token = payload["refresh_token"]
        return payload["access_token"]

    def _run_irc_loop(self, username, channel, client_id, client_secret):
        backoff = 5
        while not self._stop_event.is_set():
            try:
                access_token = self._refresh_access_token(client_id, client_secret)
                self._connect_and_listen(username, channel, access_token)
                backoff = 5  # reset after a clean-ish disconnect
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Twitch IRC connection error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _connect_and_listen(self, username, channel, token):
        raw_token = token[6:] if token.startswith('oauth:') else token
        sock = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=30)
        sock.settimeout(30)
        try:
            self._send(sock, f"PASS oauth:{raw_token}")
            self._send(sock, f"NICK {username.lower()}")
            # twitch.tv/tags carries each PRIVMSG's badges/mod fields - needed to
            # gate the mod-only short command forms below. Not awaited/ACK-checked;
            # by convention it's granted well before JOIN's own response arrives.
            self._send(sock, "CAP REQ :twitch.tv/tags")
            self._send(sock, f"JOIN #{channel.lower()}")
            self._send_chat(sock, channel,
                             f"{username} is online! Type {self._command_prefix} (or !l) <streamer> [screen1|screen2|screen3|screen4] to load a stream, "
                             f"{self._matrix_command} <screen1|screen2|screen3|screen4> for the matrix screensaver, {self._watchlist_command} (or !w) for who's on watch, "
                             f"or !commands for help.")

            buffer = ""
            while not self._stop_event.is_set():
                try:
                    data = sock.recv(4096).decode('utf-8', errors='ignore')
                except socket.timeout:
                    continue
                if not data:
                    raise ConnectionError("Twitch IRC connection closed by server")
                buffer += data
                while "\r\n" in buffer:
                    line, buffer = buffer.split("\r\n", 1)
                    self._handle_line(sock, line, channel)
        finally:
            sock.close()

    def _send(self, sock, message):
        sock.sendall((message + "\r\n").encode('utf-8'))

    def _send_chat(self, sock, channel, message):
        self._send(sock, f"PRIVMSG #{channel.lower()} :{message}")

    def _check_rate_limit(self, sock, channel):
        """Global cooldown shared by !load and !matrix - one timer, not
        per-command, so alternating between them can't dodge it. Returns True
        if the caller may proceed. On the first blocked attempt in a window,
        sends one warning reply; further spam in the same window is silent
        so the warning itself can't become spam."""
        now = time.time()
        if now - self._last_command_time < self._cooldown_seconds:
            if not self._cooldown_warned:
                remaining = round(self._cooldown_seconds - (now - self._last_command_time))
                self._send_chat(sock, channel, f"Slow down - try again in {remaining}s.")
                self._cooldown_warned = True
            return False
        self._last_command_time = now
        self._cooldown_warned = False
        return True

    def _format_watchlist_message(self):
        # cso-operator-app being briefly unreachable shouldn't take down the
        # whole IRC connection — same defensive posture as the IRC reconnect
        # backoff around this method's caller.
        try:
            req = urllib.request.Request(self._watchlist_url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                logins = json.loads(resp.read().decode('utf-8'))["logins"]
            if not logins:
                return "Currently watching: nobody right now."
            return "Currently watching: " + ", ".join(logins)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to fetch streamer watchlist: {e}")
            return "Currently watching: (couldn't reach the watchlist right now)"

    def _is_streamer_live(self, streamer):
        """Fails open (returns True) on any lookup error - an infra hiccup on
        cso-operator-app's side shouldn't silently block a real load."""
        try:
            url = f"{self._live_check_url}?login={urllib.parse.quote(streamer, safe='')}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=8) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
            return bool(payload.get("live"))
        except Exception as e:
            if self.logger:
                self.logger.error(f"Live-check failed for '{streamer}', dispatching anyway: {e}")
            return True

    # Mod-only short aliases - each checked independently against the sender's
    # privilege, not tied to which command word (long or "!l") introduced them.
    _SCREEN_SHORT = {"s1": "screen1", "s2": "screen2", "s3": "screen3", "s4": "screen4"}

    def _is_privileged(self, tags):
        """True for the channel's broadcaster or a moderator, read off the
        twitch.tv/tags IRCv3 tags requested at connect. Twitch tags the
        broadcaster via the 'badges' field, not always via 'mod' - check both."""
        if tags.get('mod') == '1':
            return True
        badges = tags.get('badges', '')
        return 'broadcaster/' in badges or 'moderator/' in badges

    def _expand_screen_token(self, token):
        """('s1'-'s4') -> ('screen1'-'screen4', True). Anything else (including
        an already-canonical screenN or a bogus token) passes through unchanged
        with was_short=False - unrecognized tokens still fall through to
        RouteOnAttribute's own 'unmatched', same as before this change."""
        low = token.lower()
        if low in self._SCREEN_SHORT:
            return self._SCREEN_SHORT[low], True
        return token, False

    def _expand_streamer_token(self, token):
        """'k:<login>' -> ('kick:<login>', True); anything else unchanged."""
        if token.lower().startswith("k:"):
            return "kick:" + token[2:], True
        return token, False

    def _parse_tags(self, line):
        """Splits a leading '@key=val;key=val ' IRCv3 tag block off the front of
        a raw line, if present. Returns (tags_dict, remaining_line)."""
        if not line.startswith('@'):
            return {}, line
        tag_str, sep, rest = line.partition(' ')
        if not sep:
            return {}, line
        tags = {}
        for kv in tag_str[1:].split(';'):
            if '=' in kv:
                k, v = kv.split('=', 1)
                tags[k] = v
        return tags, rest

    def _handle_line(self, sock, line, channel):
        tags, line = self._parse_tags(line)

        if line.startswith("PING"):
            self._send(sock, line.replace("PING", "PONG", 1))
            return

        # PRIVMSG line shape: :nick!user@host PRIVMSG #channel :message text
        if "PRIVMSG" not in line:
            return

        prefix, sep, rest = line.partition(" PRIVMSG ")
        if not sep:
            return
        nick = prefix.split("!", 1)[0].lstrip(":")
        _, _, message = rest.partition(":")
        message = message.strip()
        is_privileged = self._is_privileged(tags)

        if message.lower() in ("!commands", "!help"):
            self._send_chat(sock, channel,
                             f"Commands: {self._command_prefix} (or !l) <streamer> [screen1|screen2|screen3|screen4] - loads that stream on a screen "
                             f"(defaults to screen1) | {self._matrix_command} <screen1|screen2|screen3|screen4> - turns on the matrix screensaver "
                             f"(screen required, no default) | "
                             f"{self._watchlist_command} (or !w) - shows who's currently on the watchlist")
            return

        if message.lower() in (self._watchlist_command.lower(), "!w"):
            self._send_chat(sock, channel, self._format_watchlist_message())
            return

        tokens = message.split()
        cmd_word = tokens[0].lower() if tokens else ""

        if cmd_word in (self._matrix_command.lower(), "!m"):
            # Array-wide numbering, same as !load: screen1 -> Jetson, screen2 ->
            # GamingPC (WindowsDesktop), screen3/screen4 -> TunaStarlink's local
            # screen2/screen3 (see claude-screen.md). Unlike !load, there is no
            # default - the screen argument is required, so a bare "!matrix"/"!m"
            # or an unrecognized token doesn't match at all and is silently
            # ignored, same as an unrecognized !load screen falling through
            # unmatched. "!m" itself, and a short "s1"-"s4" screen argument, are
            # each independently mod/broadcaster-only - the long "!matrix" form
            # with a full "screenN" argument stays open to everyone, unchanged.
            if cmd_word == "!m" and not is_privileged:
                return
            if len(tokens) != 2:
                return
            arg_expanded, arg_was_short = self._expand_screen_token(tokens[1])
            if arg_was_short and not is_privileged:
                return
            arg = arg_expanded.lower()
            screen = {
                "screen1": "matrix-screen1",
                "screen2": "matrix-screen2",
                "screen3": "matrix-screen3",
                "screen4": "matrix-screen4",
            }.get(arg)
            if screen is None:
                return
            if not self._check_rate_limit(sock, channel):
                return
            # display_screen is the clean chat-facing label (TwitchChatReplyProcessor's
            # Matrix Message template uses this, not the internal routing sentinel above) -
            # always the expanded canonical form, whether the sender typed it short or long.
            display_screen = arg
            # No immediate ack here - TwitchChatReplyProcessor posts the one real
            # confirmation after the dispatch actually succeeds (was double-posting
            # before: this "loading" message plus its own "now active" reply).
            self._queue.put({
                "command": "matrix",
                "screen": screen,
                "display_screen": display_screen,
                "requested_by": nick,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
            return

        if cmd_word not in (self._command_prefix.lower(), "!l"):
            return
        if len(tokens) < 2:
            return

        if not self._check_rate_limit(sock, channel):
            return

        streamer_expanded, streamer_was_short = self._expand_streamer_token(tokens[1])
        if streamer_was_short and not is_privileged:
            return
        streamer = streamer_expanded.lstrip('@').lower()

        if len(tokens) > 2:
            screen_expanded, screen_was_short = self._expand_screen_token(tokens[2])
            if screen_was_short and not is_privileged:
                return
            screen = screen_expanded.lower()
        else:
            screen = "screen1"

        if not self._is_streamer_live(streamer):
            display_name = streamer[5:] if streamer.startswith("kick:") else streamer
            self._send_chat(sock, channel, f"{display_name} isn't live right now.")
            return

        self._queue.put({
            "command": "load",
            "streamer": streamer,
            "screen": screen,
            "requested_by": nick,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })

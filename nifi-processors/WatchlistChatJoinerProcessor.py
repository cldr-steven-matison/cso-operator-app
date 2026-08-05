# WatchlistChatJoinerProcessor.py
import socket
import time

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators


class WatchlistChatJoinerProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.5-SNAPSHOT'
        description = (
            'Holds one persistent IRC connection, opened once in onScheduled, and executes JOIN + '
            'PRIVMSG (the one-time greeting) for whichever streamer the incoming FlowFile names. Does '
            'no polling, no fan-out, no internal timers or background threads of its own - the upstream '
            'NiFi flow (GenerateFlowFile -> InvokeHTTP watchlist -> SplitJson -> live-check via Helix -> '
            'a DistributedMapCache dedup gate) is what decides *when* a FlowFile reaches this processor '
            'at all, exactly once per streamer per newly-detected join. This processor only ever does '
            'the one thing NiFi cannot do natively: hold a live authenticated Twitch IRC socket. '
            'Fully separate connection and refresh token from TwitchChatListenerProcessor - never '
            'shares state with it. Dry Run (default true) skips opening the real IRC connection '
            'entirely and logs what would be sent instead.'
        )
        tags = ['twitch', 'irc', 'chat', 'streamers', 'watchlist', 'chat-bot']
        dependencies = []

    BOT_USERNAME = PropertyDescriptor(
        name="Bot Username",
        description="Twitch login name of the bot account (e.g. tunastreettest).",
        required=True,
        default_value="tunastreettest",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    CLIENT_ID = PropertyDescriptor(
        name="Client ID",
        description="Twitch app client ID for the separate TunaStreetTestBot app "
                     "(not the app TwitchChatListenerProcessor uses).",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    CLIENT_SECRET = PropertyDescriptor(
        name="Client Secret",
        description="Twitch app client secret for the separate TunaStreetTestBot app.",
        required=True,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    REFRESH_TOKEN = PropertyDescriptor(
        name="Refresh Token",
        description="Independent user refresh token for the bot account (chat:read+chat:edit scopes). "
                     "Do NOT reuse TwitchChatListenerProcessor's refresh token - Twitch rotates it on "
                     "every use and two processors refreshing from the same seed will race each other.",
        required=True,
        sensitive=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    GREETING_MESSAGE = PropertyDescriptor(
        name="Greeting Message",
        description="Posted once, right after joining a streamer's channel.",
        required=True,
        default_value="\U0001F41F I am Tuna \U0001F44B You are on my WatchList \U0001F3AC",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    STREAMER_ATTRIBUTE = PropertyDescriptor(
        name="Streamer Attribute",
        description="FlowFile attribute holding the Twitch login to join.",
        required=True,
        default_value="streamer",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )
    DRY_RUN = PropertyDescriptor(
        name="Dry Run",
        description="When true (default), never opens a real IRC connection - logs what would be "
                     "sent instead. Must be explicitly set to false to join/post for real.",
        required=True,
        default_value="true",
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
    )

    def __init__(self, **kwargs):
        # 'pass' is the safest initialization in many containerized environments —
        # real state is set up in onScheduled, which is guaranteed to run before transform().
        pass

    def getPropertyDescriptors(self):
        return [
            self.BOT_USERNAME, self.CLIENT_ID, self.CLIENT_SECRET, self.REFRESH_TOKEN,
            self.GREETING_MESSAGE, self.STREAMER_ATTRIBUTE, self.DRY_RUN,
        ]

    def onScheduled(self, context):
        self._dry_run = context.getProperty(self.DRY_RUN).asBoolean()
        self._greeting = context.getProperty(self.GREETING_MESSAGE).getValue()
        self._username = context.getProperty(self.BOT_USERNAME).getValue()
        self._client_id = context.getProperty(self.CLIENT_ID).getValue()
        self._client_secret = context.getProperty(self.CLIENT_SECRET).getValue()
        # Seeded from the property once; rotates in-memory on every refresh after that,
        # same discipline as TwitchChatListenerProcessor. Its own independent seed -
        # never TwitchChatListenerProcessor's twitch-bot-refresh-token.
        self._refresh_token = context.getProperty(self.REFRESH_TOKEN).getValue()
        self._sock = None
        # Already-joined-this-session dedup, belt-and-suspenders alongside the upstream
        # DistributedMapCache gate - a restart of this processor alone (bundle-version
        # switch, etc.) shouldn't cause a duplicate JOIN+greet within the same session.
        self._joined = set()

    def onStopped(self, context):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def transform(self, context, flowfile):
        attributes = dict(flowfile.getAttributes())
        streamer_attr = context.getProperty(self.STREAMER_ATTRIBUTE).evaluateAttributeExpressions(flowfile).getValue()
        streamer = attributes.get(streamer_attr, '').strip().lstrip('#').lower()

        if not streamer:
            attributes['join_error'] = f"No value found for attribute '{streamer_attr}'"
            return FlowFileTransformResult(relationship='failure', attributes=attributes)

        if streamer in self._joined:
            attributes['join_result'] = 'already_joined_this_session'
            return FlowFileTransformResult(relationship='success', attributes=attributes)

        if self._dry_run:
            if self.logger:
                self.logger.info(f"[dry run] would JOIN #{streamer} and greet: {self._greeting}")
            self._joined.add(streamer)
            attributes['dry_run'] = 'true'
            return FlowFileTransformResult(relationship='success', attributes=attributes)

        try:
            self._ensure_connected()
            self._send(f"JOIN #{streamer}")
            self._send(f"PRIVMSG #{streamer} :{self._greeting}")
            self._joined.add(streamer)
            attributes['dry_run'] = 'false'
            attributes['join_result'] = 'joined'
            return FlowFileTransformResult(relationship='success', attributes=attributes)
        except Exception as e:
            if self.logger:
                self.logger.error(f"WatchlistChatJoinerProcessor failed to join #{streamer}: {e}")
            # Force a reconnect on the next FlowFile rather than keep using a socket
            # that may be in a bad state.
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
            attributes['join_error'] = str(e)
            return FlowFileTransformResult(relationship='failure', attributes=attributes)

    # --- IRC connection handling ---

    def _ensure_connected(self):
        if self._sock is not None:
            return
        access_token = self._refresh_access_token()
        self._sock = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=15)
        self._sock.settimeout(15)
        self._send(f"PASS oauth:{access_token}")
        self._send(f"NICK {self._username.lower()}")
        # Drain the connection registration burst (NOTICE/001-004/CAP lines) so a later
        # recv() during PING handling doesn't trip over leftover bytes from login.
        self._drain(timeout=2)

    def _refresh_access_token(self):
        import json
        import urllib.parse
        import urllib.request
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }).encode()
        req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=body, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        self._refresh_token = payload["refresh_token"]
        return payload["access_token"]

    def _send(self, message):
        self._sock.sendall((message + "\r\n").encode('utf-8'))

    def _drain(self, timeout):
        self._sock.settimeout(timeout)
        try:
            while True:
                data = self._sock.recv(4096)
                if not data:
                    break
        except socket.timeout:
            pass
        finally:
            self._sock.settimeout(15)

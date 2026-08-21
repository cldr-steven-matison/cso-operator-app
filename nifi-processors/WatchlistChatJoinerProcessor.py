# WatchlistChatJoinerProcessor.py
import socket
import time

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators


class WatchlistChatJoinerProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.6-SNAPSHOT'
        description = (
            'Holds one persistent IRC connection, opened once in onScheduled, and executes JOIN + '
            'PRIVMSG (the one-time greeting) for whichever streamer the incoming FlowFile names. Does '
            'no polling, no fan-out, no internal timers or background threads of its own - the upstream '
            'NiFi flow (GenerateFlowFile -> InvokeHTTP watchlist -> SplitJson -> live-check via Helix -> '
            'a DistributedMapCache dedup gate) is what decides *when* a FlowFile reaches this processor '
            'at all, exactly once per streamer per newly-detected join. This processor only ever does '
            'the one thing NiFi cannot do natively: hold a live authenticated Twitch IRC socket. '
            'Fully separate connection and refresh token from TwitchChatListenerProcessor - never '
            'shares state with it. Twitch rotates the refresh token on every use, so the rotated '
            'value is persisted to NiFi component state (Scope.LOCAL, key "refresh_token") and read '
            'back on the next onScheduled - a restart no longer needs a manual device-code re-auth. '
            'State is per processor instance, so two instances of this class (WatchlistChatJoiner and '
            'TopStreamerJoiner) keep separate tokens for their separate Twitch apps. '
            'Dry Run (default true) skips opening the real IRC connection '
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
                     "every use and two processors refreshing from the same seed will race each other. "
                     "This is a SEED only: it is read on the first start and whenever component state is "
                     "empty, after which the rotated token is persisted to state and this property is "
                     "ignored. To force a re-seed, paste a freshly minted token here (or in the Parameter "
                     "Context) and restart - a dead stored token is dropped automatically on HTTP 400.",
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

    # Component-state key holding the rotated refresh token. NiFi scopes component state per
    # processor instance, so the two instances of this class do not collide.
    STATE_KEY_REFRESH_TOKEN = 'refresh_token'

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
        # The property is a seed, not the token in ongoing use: Twitch rotates the refresh
        # token on every use, so the live value lives in component state and the property is
        # only consulted when state is empty (first ever start, or after a deliberate re-seed).
        # Its own independent seed - never TwitchChatListenerProcessor's twitch-bot-refresh-token.
        # Guarded: a NiFi build without the state binding must degrade to the old
        # property-seed behaviour, not fail to start the processor at all.
        try:
            self._state_manager = context.getStateManager()
        except Exception as e:
            self._state_manager = None
            if self.logger:
                self.logger.warn(f"Component state unavailable; the rotated Twitch refresh token "
                                 f"will not survive a restart: {e}")
        self._property_seed = context.getProperty(self.REFRESH_TOKEN).getValue()
        self._reseed_attempted = False
        stored = self._read_stored_refresh_token()
        if stored:
            self._refresh_token = stored
            self._token_source = 'state'
        else:
            self._refresh_token = self._property_seed
            self._token_source = 'property'
        if self.logger:
            self.logger.info(f"Twitch refresh token seeded from {self._token_source}")
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
        import urllib.error
        try:
            return self._request_access_token()
        except urllib.error.HTTPError as e:
            # 400 here means the refresh token itself is dead, not that Twitch is unreachable.
            # If the dead one came out of component state, drop it and give the property seed
            # exactly one chance - that makes re-seeding "paste a fresh token into the Parameter
            # Context and restart" instead of a code change. Only once per run: retrying a seed
            # that is itself spent just burns calls and muddies the log.
            if e.code != 400 or self._token_source != 'state' or self._reseed_attempted:
                raise
            self._reseed_attempted = True
            if self.logger:
                self.logger.warn("Persisted Twitch refresh token was rejected (HTTP 400); "
                                 "clearing component state and retrying once from the property seed")
            self._clear_stored_refresh_token()
            self._refresh_token = self._property_seed
            self._token_source = 'property'
            return self._request_access_token()

    def _request_access_token(self):
        import json
        import urllib.error
        import urllib.parse
        import urllib.request
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }).encode()
        req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=body, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            # Without this, transform()'s broad except reports a dead token identically to a
            # network blip, and a botched deploy looks exactly like Twitch being unreachable.
            # Log Twitch's own body. Same handling as TwitchChatListenerProcessor.
            detail = e.read().decode('utf-8', errors='ignore')[:500]
            if self.logger:
                self.logger.error(f"Twitch token refresh rejected: HTTP {e.code} {detail}")
            raise
        if "access_token" not in payload:
            raise RuntimeError(f"Twitch token refresh returned no access_token: {json.dumps(payload)[:500]}")
        # Twitch rotates the refresh token on every use - the old one is now invalid. It has
        # been observed absent on some responses; keeping the previous value is strictly better
        # than a KeyError that reads as a connection failure.
        rotated = payload.get("refresh_token")
        if rotated:
            self._refresh_token = rotated
            self._token_source = 'state'
            # Safe to write straight through: unlike TwitchChatListenerProcessor, this whole
            # path runs on the NiFi task thread under transform(), not a background thread.
            self._persist_refresh_token(rotated)
        elif self.logger:
            self.logger.warn("Twitch token refresh returned no refresh_token; keeping the previous one")
        return payload["access_token"]

    # --- Component state: the rotated refresh token ---
    #
    # Imported lazily rather than at module scope: nifiapi.componentstate resolves
    # Scope.LOCAL/CLUSTER through the py4j JVM bridge at import time.
    # State is not encrypted the way a sensitive property is, and this pod's volumes are
    # emptyDir - so this survives a processor or NiFi restart, but not a pod delete. A pod
    # delete already destroys the entire flow, so that is no worse than the flow's own
    # durability. Every one of these is best-effort: a state failure must never take down a
    # join, since the in-memory token still works for the life of the process.

    def _read_stored_refresh_token(self):
        if self._state_manager is None:
            return None
        try:
            from nifiapi.componentstate import Scope
            return self._state_manager.getState(Scope.LOCAL).get(self.STATE_KEY_REFRESH_TOKEN)
        except Exception as e:
            if self.logger:
                self.logger.warn(f"Could not read the persisted Twitch refresh token from state, "
                                 f"falling back to the property seed: {e}")
            return None

    def _persist_refresh_token(self, token):
        if self._state_manager is None:
            return
        try:
            from nifiapi.componentstate import Scope
            state = self._state_manager.getState(Scope.LOCAL).toMap()
            state[self.STATE_KEY_REFRESH_TOKEN] = token
            self._state_manager.setState(state, Scope.LOCAL)
        except Exception as e:
            if self.logger:
                self.logger.warn(f"Could not persist the rotated Twitch refresh token; this run is "
                                 f"fine but the next restart will need a re-seed: {e}")

    def _clear_stored_refresh_token(self):
        if self._state_manager is None:
            return
        try:
            from nifiapi.componentstate import Scope
            self._state_manager.clear(Scope.LOCAL)
        except Exception as e:
            if self.logger:
                self.logger.warn(f"Could not clear the rejected Twitch refresh token from state: {e}")

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

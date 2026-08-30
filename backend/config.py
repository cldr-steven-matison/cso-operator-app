from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")

    VLLM_URL: str = "http://vllm-service.default.svc.cluster.local:8000"
    # Must match what vLLM is actually serving — check `GET /v1/models`.
    VLLM_MODEL: str = "Qwen/Qwen2.5-3B-Instruct"
    # Shadow mode (#277): the DGX Spark caption brain's HTTP door. Empty = off
    # (default). When set, process_clip also POSTs each clip there and stores the
    # reply as `brain_caption` beside `caption` — never promoted, never blocking.
    BRAIN_DOOR_URL: str = ""
    BRAIN_DOOR_TIMEOUT: float = 60.0

    QDRANT_URL: str = "http://qdrant.default.svc.cluster.local:6333"
    QDRANT_COLLECTION: str = "my-rag-collection"

    EMBED_URL: str = "http://embedding-server-service.default.svc.cluster.local:80"
    EMBED_DIM: int = 768

    WHISPER_URL: str = "http://whisper-service.default.svc.cluster.local:8001"

    NIFI_URL: str = "https://mynifi-web.cfm-streaming.svc.cluster.local"
    NIFI_VERIFY_TLS: bool = False
    NIFI_USERNAME: str = ""
    NIFI_PASSWORD: str = ""
    # mTLS client cert for a userCertAuth NiFi (prod on cso-prod-1). When both are set the
    # httpx client presents the cert and the Bearer-token path is skipped entirely.
    NIFI_CLIENT_CERT: str = ""
    NIFI_CLIENT_KEY: str = ""

    @property
    def nifi_client_cert(self) -> tuple[str, str] | None:
        if self.NIFI_CLIENT_CERT and self.NIFI_CLIENT_KEY:
            return (self.NIFI_CLIENT_CERT, self.NIFI_CLIENT_KEY)
        return None

    @property
    def nifi_verify(self):
        """The `verify=` argument for an httpx client talking to NiFi.

        httpx >= 0.28 removed the `cert=` parameter; the client cert has to ride on an
        ssl.SSLContext passed as `verify=`. Without a client cert this is just the bool.
        """
        cert = self.nifi_client_cert
        if cert is None:
            return self.NIFI_VERIFY_TLS
        import ssl

        ctx = ssl.create_default_context()
        if not self.NIFI_VERIFY_TLS:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        ctx.load_cert_chain(certfile=cert[0], keyfile=cert[1])
        return ctx

    KAFKA_BOOTSTRAP: str = "my-cluster-kafka-bootstrap.cld-streaming.svc:9092"
    TOPIC_AUDIO: str = "new_audio"
    TOPIC_DOCS: str = "new_documents"

    # Single NiFi ListenHTTP at the head of IngestDataToStream.
    # The flow's RouteOnAttribute branches docs vs audio by Content-Type / mime.
    NIFI_INGEST_URL: str = "http://mynifi.cfm-streaming.svc.cluster.local:9000/contentListener"

    # StreamersApp's shared on-demand entry point: a single ListenHTTP ("Trigger")
    # feeds RouteOnAttribute, which branches on the X-Trigger-Request header to
    # LiveStreamerAlert / FetchClips / PublishClipPeakTimeCron's TriggerInput port.
    # One flowfile through, bypassing each flow's own top-level scheduler.
    NIFI_TRIGGER_URL: str = "http://mynifi.cfm-streaming.svc.cluster.local:9080/contentListener"

    EFM_URL: str = "http://efm.cld-streaming.svc:10090"

    # EFM's own Postgres — direct read of the agent/device tables for a real
    # agent registry, replacing the operations/events discovery heuristic
    # (EFM v2.3.1 has no REST "list agents" endpoint, and its operations table
    # has no automatic retention — confirmed 2026-07-18 when a single agent's
    # reconnect-loop piled up ~11.8k rows in under a day and made that endpoint
    # hang entirely, which in turn made agents vanish from this heuristic).
    EFM_DB_HOST: str = "ssb-postgresql.cld-streaming.svc.cluster.local"
    EFM_DB_PORT: int = 5432
    EFM_DB_NAME: str = "efm"
    EFM_DB_USER: str = ""
    EFM_DB_PASSWORD: str = ""

    # Streamers roster/catalog store (#275) — a dedicated `streamers` database on
    # the same ssb-postgresql server, its own role. Replaces the hardcoded
    # _TWITCH_LOGINS/_KICK_LOGINS/_STREAMER_CATALOG/_STREAMER_PATH_OVERRIDES in
    # services/streamers.py as the source of truth; those constants remain the
    # seed and the fallback when this DB is unreachable. Creds arrive via
    # `kubectl set env`, never YAML. An empty user disables the store entirely.
    STREAMERS_DB_HOST: str = "ssb-postgresql.cld-streaming.svc.cluster.local"
    STREAMERS_DB_PORT: int = 5432
    STREAMERS_DB_NAME: str = "streamers"
    STREAMERS_DB_USER: str = ""
    STREAMERS_DB_PASSWORD: str = ""

    # URL for "Use sample audio" — proxied through the backend to dodge CORS.
    SAMPLE_AUDIO_URL: str = (
        "https://www.voiptroubleshooter.com/open_speech/american/OSR_us_000_0010_8k.wav"
    )

    # RAG knobs
    RAG_TOP_K: int = 4
    RAG_MAX_TOKENS: int = 512

    # Optional modules baked into this image (comma-separated, e.g. "streamers")
    MODULES: str = ""

    # Streamers module — Twitch clip pipeline
    TWITCH_CLIENT_ID: str = ""
    TWITCH_CLIENT_SECRET: str = ""
    CLIP_STORAGE_PATH: str = "/clips"
    NEW_CLIPS_TOPIC: str = "new_clips"
    PROCESSED_CLIPS_TOPIC: str = "processed_clips"
    TOPIC_CHAT_ACTIVITY: str = "twitch_chat_activity"

    # Toggle for the glitch-intro burn in _fetch_twitch_clips/_fetch_kick_clips.
    # Set false to pause it without touching the ffmpeg pipeline itself.
    GLITCH_INTRO_ENABLED: bool = True

    # Kick API — OAuth2 client credentials
    KICK_CLIENT_ID: str = ""
    KICK_CLIENT_SECRET: str = ""

    # X (Twitter) API — OAuth 1.0a, @TunaStreetTest
    X_API_KEY: str = ""
    X_API_SECRET: str = ""
    X_ACCESS_TOKEN: str = ""
    X_ACCESS_TOKEN_SECRET: str = ""


settings = Settings()

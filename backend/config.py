from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")

    VLLM_URL: str = "http://vllm-service.default.svc.cluster.local:8000"
    # Must match what vLLM is actually serving — check `GET /v1/models`.
    VLLM_MODEL: str = "Qwen/Qwen2.5-3B-Instruct"

    QDRANT_URL: str = "http://qdrant.default.svc.cluster.local:6333"
    QDRANT_COLLECTION: str = "my-rag-collection"

    EMBED_URL: str = "http://embedding-server-service.default.svc.cluster.local:80"
    EMBED_DIM: int = 768

    WHISPER_URL: str = "http://whisper-service.default.svc.cluster.local:8001"

    NIFI_URL: str = "https://mynifi-web.cfm-streaming.svc.cluster.local"
    NIFI_VERIFY_TLS: bool = False
    NIFI_USERNAME: str = ""
    NIFI_PASSWORD: str = ""

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

    # Kick API — OAuth2 client credentials
    KICK_CLIENT_ID: str = ""
    KICK_CLIENT_SECRET: str = ""

    # X (Twitter) API — OAuth 1.0a, @TunaStreetTest
    X_API_KEY: str = ""
    X_API_SECRET: str = ""
    X_ACCESS_TOKEN: str = ""
    X_ACCESS_TOKEN_SECRET: str = ""


settings = Settings()

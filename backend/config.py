from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")

    VLLM_URL: str = "http://vllm-service.default.svc.cluster.local:8000"
    # Must match what vLLM is actually serving — check `GET /v1/models`.
    VLLM_MODEL: str = "Qwen/Qwen2.5-1.5B-Instruct"

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

    EFM_URL: str = "http://efm.cld-streaming.svc:10090"

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
    STREAMERS_WATCH_LIST: str = ""      # comma-separated Twitch logins
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

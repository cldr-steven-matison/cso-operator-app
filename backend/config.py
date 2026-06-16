from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")

    VLLM_URL: str = "http://vllm-service.default.svc.cluster.local:8000"
    VLLM_MODEL: str = "Qwen/Qwen2.5-3B-Instruct"

    QDRANT_URL: str = "http://qdrant.default.svc.cluster.local:6333"
    QDRANT_COLLECTION: str = "my-rag-collection"

    EMBED_URL: str = "http://embedding-server-service.default.svc.cluster.local:80"
    EMBED_DIM: int = 768

    WHISPER_URL: str = "http://whisper-service.default.svc.cluster.local:8001"

    NIFI_URL: str = "https://mynifi-web.mynifi.cfm-streaming.svc.cluster.local"
    NIFI_VERIFY_TLS: bool = False
    NIFI_USERNAME: str = ""
    NIFI_PASSWORD: str = ""

    KAFKA_BOOTSTRAP: str = "my-cluster-kafka-bootstrap.cld-streaming.svc:9092"
    TOPIC_AUDIO: str = "new_audio"
    TOPIC_DOCS: str = "new_documents"

    # NiFi ListenHTTP endpoints (set after the processors are wired in the flows)
    NIFI_INGEST_DOC_URL: str = ""
    NIFI_INGEST_AUDIO_URL: str = ""

    # RAG knobs
    RAG_TOP_K: int = 4
    RAG_MAX_TOKENS: int = 512


settings = Settings()

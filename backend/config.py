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

    KAFKA_BOOTSTRAP: str = "my-cluster-kafka-bootstrap.cld-streaming.svc:9092"
    TOPIC_AUDIO: str = "new_audio"
    TOPIC_DOCS: str = "new_documents"


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STREAM_")

    kafka_bootstrap_servers: str = "localhost:9092"
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    database_url: str = "sqlite:///./streaming.db"

    raw_events_topic: str = "raw-events"
    enriched_events_topic: str = "enriched-events"
    fraud_alerts_topic: str = "fraud-alerts"
    dead_letter_topic: str = "dead-letter"

    dedup_ttl_seconds: int = 3600
    window_size_seconds: int = 60


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "order"
    database_url: str
    kafka_bootstrap_servers: str = "localhost:9092"
    otel_exporter_otlp_endpoint: str = "http://collector:4318"
    outbox_poll_interval: float = 0.5


settings = Settings()

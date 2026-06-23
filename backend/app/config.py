from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    DATABASE_URL: str

    AMAZON_SP_CLIENT_ID: str = ""
    AMAZON_SP_CLIENT_SECRET: str = ""
    AMAZON_SP_REFRESH_TOKEN: str = ""

    AMAZON_ADS_CLIENT_ID: str = ""
    AMAZON_ADS_CLIENT_SECRET: str = ""
    AMAZON_ADS_REFRESH_TOKEN: str = ""

    SHOPIFY_STORE_URL: str = ""
    SHOPIFY_API_KEY: str = ""
    SHOPIFY_API_SECRET: str = ""
    SHOPIFY_ACCESS_TOKEN: str = ""

    META_ADS_ACCESS_TOKEN: str = ""
    META_ADS_ACCOUNT_ID: str = ""

    ANTHROPIC_API_KEY: str = ""

    ETL_SCHEDULE_ENABLED: bool = True

    CORS_ORIGINS: str = "http://localhost:3000"

    @property
    def async_database_url(self) -> str:
        url = self.DATABASE_URL
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://", 1)
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql+asyncpg://", 1)
        return url

    @property
    def sync_database_url(self) -> str:
        url = self.DATABASE_URL
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql://", 1)
        return url

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


settings = Settings()

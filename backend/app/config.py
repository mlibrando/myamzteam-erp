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
    AMAZON_SP_REFRESH_TOKEN_NA: str = ""
    AMAZON_SP_REFRESH_TOKEN_EU: str = ""
    AMAZON_SP_REFRESH_TOKEN_FE: str = ""

    AMAZON_ADS_CLIENT_ID: str = ""
    AMAZON_ADS_CLIENT_SECRET: str = ""
    # NA is the primary Ads refresh token. EU and FE are OPTIONAL: when unset,
    # the connector falls back to the NA token. A single LWA authorization
    # spans all three regions when the seller login has access to marketplaces
    # in each — verified for the MagicalButter account 2026-07 (see
    # AmazonAdsConnector._refresh_token_for). Set the regional vars
    # explicitly only if the seller has separated accounts per region and
    # each region needs its own OAuth flow.
    AMAZON_ADS_REFRESH_TOKEN_NA: str = ""
    AMAZON_ADS_REFRESH_TOKEN_EU: str = ""
    AMAZON_ADS_REFRESH_TOKEN_FE: str = ""

    SHOPIFY_STORE_URL: str = ""
    SHOPIFY_API_KEY: str = ""
    SHOPIFY_API_SECRET: str = ""
    SHOPIFY_ACCESS_TOKEN: str = ""

    META_ADS_ACCESS_TOKEN: str = ""
    META_ADS_ACCOUNT_ID: str = ""

    ANTHROPIC_API_KEY: str = ""

    ETL_SCHEDULE_ENABLED: bool = True

    # All monthly P&L cutoffs use this timezone. Amazon Seller Central
    # defaults reporting cutoffs to Pacific Time regardless of the seller's
    # marketplace region, and Elena's manual P&L follows that convention.
    # Daily bucketing in pnl_calculator groups events by (posted_date AT
    # TIME ZONE THIS) so daily_pnl.date matches what Elena sees in her
    # spreadsheet. Change per-marketplace only if a future seller onboards
    # with a different reporting timezone.
    MONTHLY_CUTOFF_TIMEZONE: str = "America/Los_Angeles"

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

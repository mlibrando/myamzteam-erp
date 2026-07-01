import os
import sys
from pathlib import Path

# Ensure backend/ is importable as the project root for `app.*` imports.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

# Provide a placeholder DATABASE_URL so app.config.Settings() doesn't fail at import
# time. Tests never connect to it.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

# Tests must NOT read the developer's local .env — some tests assert the
# "no credentials configured" path raises AuthError, which regresses whenever
# a live token happens to be loaded (e.g. via a backend/.env symlink for
# scripts). Force every credential-shaped Settings field to empty via
# os.environ (env vars beat .env file in pydantic-settings priority order).
for _var in (
    "AMAZON_SP_CLIENT_ID",
    "AMAZON_SP_CLIENT_SECRET",
    "AMAZON_SP_REFRESH_TOKEN_NA",
    "AMAZON_SP_REFRESH_TOKEN_EU",
    "AMAZON_SP_REFRESH_TOKEN_FE",
    "AMAZON_ADS_CLIENT_ID",
    "AMAZON_ADS_CLIENT_SECRET",
    "AMAZON_ADS_REFRESH_TOKEN_NA",
    "AMAZON_ADS_REFRESH_TOKEN_EU",
    "AMAZON_ADS_REFRESH_TOKEN_FE",
    "SHOPIFY_STORE_URL",
    "SHOPIFY_API_KEY",
    "SHOPIFY_API_SECRET",
    "SHOPIFY_ACCESS_TOKEN",
    "META_ADS_ACCESS_TOKEN",
    "META_ADS_ACCOUNT_ID",
    "ANTHROPIC_API_KEY",
):
    os.environ[_var] = ""

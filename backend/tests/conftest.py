import os
import sys
from pathlib import Path

# Ensure backend/ is importable as the project root for `app.*` imports.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

# Provide a placeholder DATABASE_URL so app.config.Settings() doesn't fail at import
# time. Tests never connect to it.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

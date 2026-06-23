from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db

router = APIRouter()


@router.get("/health")
async def health(db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    db_status = "ok"
    db_error: str | None = None
    try:
        result = await db.execute(text("SELECT 1"))
        result.scalar_one()
    except Exception as exc:
        db_status = "error"
        db_error = str(exc)

    status = "ok" if db_status == "ok" else "degraded"
    payload: dict[str, object] = {
        "status": status,
        "database": db_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if db_error:
        payload["database_error"] = db_error
    return payload

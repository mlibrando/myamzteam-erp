from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.etl.scheduler import get_scheduler_status

router = APIRouter()


@router.get("/scheduler/status")
async def scheduler_status() -> dict[str, Any]:
    """Return the current scheduler job list and next run times."""
    return get_scheduler_status()

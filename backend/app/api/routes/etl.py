from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.amazon_sp import MARKETPLACE_REGION
from app.database import get_db
from app.etl.amazon_etl import ALL_MARKETPLACES, run_amazon_etl
from app.etl.pnl_calculator import calculate_daily_pnl

logger = logging.getLogger(__name__)

router = APIRouter()


class AmazonEtlRequest(BaseModel):
    start_date: date
    end_date: date
    marketplace_ids: list[str] | None = Field(
        default=None,
        description="If omitted, runs for all five marketplaces.",
    )
    skip_pnl_calc: bool = False

    @field_validator("end_date")
    @classmethod
    def _end_after_start(cls, v: date, info: Any) -> date:
        start = info.data.get("start_date")
        if start and v < start:
            raise ValueError("end_date must be >= start_date")
        return v

    @field_validator("marketplace_ids")
    @classmethod
    def _validate_marketplaces(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        unknown = [m for m in v if m not in MARKETPLACE_REGION]
        if unknown:
            raise ValueError(f"unknown marketplace_ids: {unknown}")
        return v


class AmazonEtlResponse(BaseModel):
    etl: dict[str, Any]
    pnl: dict[str, Any]


@router.post("/etl/amazon/run", response_model=AmazonEtlResponse)
async def run_amazon(
    request: AmazonEtlRequest,
    db: AsyncSession = Depends(get_db),
) -> AmazonEtlResponse:
    marketplaces = request.marketplace_ids or list(ALL_MARKETPLACES)
    logger.info(
        "etl_route start=%s end=%s marketplaces=%s",
        request.start_date,
        request.end_date,
        marketplaces,
    )
    try:
        etl_summary = await run_amazon_etl(
            db,
            start_date=request.start_date,
            end_date=request.end_date,
            marketplace_ids=marketplaces,
        )
    except Exception as exc:
        await db.rollback()
        logger.exception("etl_route failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"amazon ETL failed: {exc}",
        ) from exc

    pnl_summary: dict[str, Any] = {"skipped": True}
    if not request.skip_pnl_calc:
        pnl_calc = await calculate_daily_pnl(
            db,
            start_date=request.start_date,
            end_date=request.end_date,
            marketplace_ids=marketplaces,
        )
        pnl_summary = {
            "rows_written": pnl_calc.rows_written,
            "skus_without_cogs": pnl_calc.skus_without_cogs,
        }

    await db.commit()
    return AmazonEtlResponse(etl=etl_summary.to_dict(), pnl=pnl_summary)

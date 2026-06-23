import uuid
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import DECIMAL, Date, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class AdSpend(Base, TimestampMixin):
    __tablename__ = "ad_spend"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    platform: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    campaign_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    campaign_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    marketplace: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    spend: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    sales_attributed: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    impressions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clicks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    acos: Mapped[Decimal | None] = mapped_column(DECIMAL(5, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

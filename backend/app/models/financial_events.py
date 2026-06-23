import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DECIMAL, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class FinancialEvent(Base, TimestampMixin):
    __tablename__ = "financial_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    event_group_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    posted_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    marketplace_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    order_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    asin: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    sku: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    fee_type: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    fee_amount: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

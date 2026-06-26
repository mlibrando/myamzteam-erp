import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DECIMAL, DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class UnmappedLineItem(Base):
    __tablename__ = "unmapped_line_items"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    posted_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    marketplace_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    line_item_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    amount: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    source_endpoint: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

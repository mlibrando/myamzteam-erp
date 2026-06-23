import uuid
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import DECIMAL, Date, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class ShopifySale(Base, TimestampMixin):
    __tablename__ = "shopify_sales"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    order_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    total_price: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    subtotal: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    total_tax: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    total_discounts: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import DECIMAL, Date, DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ProductCogs(Base):
    __tablename__ = "product_cogs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    marketplace: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    sku: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    asin: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    product_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    unit_cost: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False)
    product_price: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    effective_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

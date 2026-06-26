import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DECIMAL, DateTime, Integer, String
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
    # Mapped PnlCategory value (sales, cogs, ad_spend, selling_fees,
    # operational_fees, refunds, reimbursements). Null when the raw item
    # could not be mapped — those items are also recorded in
    # unmapped_line_items so they can be reviewed.
    category: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    # Signed for aggregation. Sales/Reimbursements use raw signs (positive in,
    # negative reduces). Cost categories store the absolute outflow as positive
    # (so the P&L formula's subtraction works), with reversals negative.
    fee_amount: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    # Raw value as returned by SP-API, sign-preserved, for debugging.
    raw_amount: Mapped[Decimal | None] = mapped_column(DECIMAL(12, 2), nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

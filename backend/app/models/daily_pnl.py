import uuid
from datetime import date
from decimal import Decimal

from sqlalchemy import DECIMAL, Date, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class DailyPnL(Base, TimestampMixin):
    __tablename__ = "daily_pnl"
    __table_args__ = (
        UniqueConstraint("date", "channel", name="uq_daily_pnl_date_channel"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    sales: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    cogs: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)

    ad_spend: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    ad_spend_sp: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    ad_spend_sb: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    ad_spend_sd: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    ad_spend_sv: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)

    selling_fees: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    operational_fees: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    refunds: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    reimbursements: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)

    gross_profit_no_reimb: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    gross_profit_with_reimb: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    margin_pct: Mapped[Decimal] = mapped_column(DECIMAL(5, 2), nullable=False, default=0)

    sales_usd: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    gross_profit_usd: Mapped[Decimal] = mapped_column(DECIMAL(12, 2), nullable=False, default=0)
    fx_rate: Mapped[Decimal] = mapped_column(DECIMAL(10, 6), nullable=False, default=1)

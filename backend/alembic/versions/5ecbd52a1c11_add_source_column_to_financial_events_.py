"""add source column to financial_events for reconciliation

Distinguishes rows produced by the three SP-API paths that feed
financial_events. The reconciliation ETL uses this column to safely
replace less-authoritative rows (by-date) with more-authoritative ones
(by-group, then settlement) for the same (marketplace, PT-local date)
window without touching data from a different source or a different
day.

- financial_events_by_date   -> daily amazon_etl (Reports API by-date)
- financial_events_by_group  -> monthly reconciliation via Closed groups
- settlement                 -> settlement report ingestion (Taxes remitted)

Revision ID: 5ecbd52a1c11
Revises: afd15f401d7a
Create Date: 2026-07-01 22:15:14.327356
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5ecbd52a1c11"
down_revision: Union[str, None] = "afd15f401d7a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add nullable column so the ALTER doesn't require a table lock long
    #    enough to backfill a large table synchronously.
    op.add_column(
        "financial_events",
        sa.Column("source", sa.String(length=40), nullable=True),
    )
    # 2. Backfill existing rows to the daily-ETL source label.
    op.execute(
        "UPDATE financial_events SET source = 'financial_events_by_date' "
        "WHERE source IS NULL"
    )
    # 3. Now enforce NOT NULL + default for future inserts.
    op.alter_column(
        "financial_events",
        "source",
        existing_type=sa.String(length=40),
        nullable=False,
        server_default=sa.text("'financial_events_by_date'"),
    )
    # 4. Index on (marketplace_id, source, posted_date) — the reconciliation
    #    ETL's DELETE filter uses these three fields together.
    op.create_index(
        "ix_financial_events_marketplace_source_posted",
        "financial_events",
        ["marketplace_id", "source", "posted_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_financial_events_marketplace_source_posted", "financial_events")
    op.drop_column("financial_events", "source")

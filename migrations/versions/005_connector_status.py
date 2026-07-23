"""add connector_status for per-connector dual-write"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "005_connector_status"
down_revision: str | None = "004_active_session_unique"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "connector_status",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("charger_id", sa.Integer(), nullable=False),
        sa.Column("connector_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["charger_id"], ["chargers.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "charger_id",
            "connector_id",
            name="uq_connector_status_charger_connector",
        ),
    )


def downgrade() -> None:
    op.drop_table("connector_status")

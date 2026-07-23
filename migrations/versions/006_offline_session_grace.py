"""offline session auto-close after WS disconnect grace period"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "006_offline_session_grace"
down_revision: str | None = "005_connector_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "chargers",
        sa.Column("disconnected_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "sessions",
        sa.Column("end_reason", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "sessions",
        sa.Column(
            "meter_stop_estimated",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("sessions", "meter_stop_estimated")
    op.drop_column("sessions", "end_reason")
    op.drop_column("chargers", "disconnected_at")

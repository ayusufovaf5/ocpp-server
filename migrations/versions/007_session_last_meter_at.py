"""track last MeterValues activity for charging session timeout"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "007_session_last_meter_at"
down_revision: str | None = "006_offline_session_grace"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sessions",
        sa.Column("last_meter_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("sessions", "last_meter_at")

"""partial unique index: one Active session per charger connector"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "004_active_session_unique"
down_revision: str | None = "003_nullable_ocpp_transaction_id"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX_NAME = "uq_sessions_active_charger_connector"


def upgrade() -> None:
    op.create_index(
        INDEX_NAME,
        "sessions",
        ["charger_id", "connector_id"],
        unique=True,
        postgresql_where=sa.text("status = 'Active'"),
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="sessions")

"""allow null ocpp_transaction_id until id is assigned after insert"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "003_nullable_ocpp_transaction_id"
down_revision: str | None = "002_sessions_meter_values"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "sessions",
        "ocpp_transaction_id",
        existing_type=sa.Integer(),
        nullable=True,
    )


def downgrade() -> None:
    op.execute(
        sa.text("UPDATE sessions SET ocpp_transaction_id = id WHERE ocpp_transaction_id IS NULL")
    )
    op.alter_column(
        "sessions",
        "ocpp_transaction_id",
        existing_type=sa.Integer(),
        nullable=False,
    )

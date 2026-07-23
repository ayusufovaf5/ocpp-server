"""extend chargers; add sessions and meter_values"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002_sessions_meter_values"
down_revision: str | None = "001_create_chargers"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "chargers",
        sa.Column("charge_point_id", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "chargers",
        sa.Column("connector_count", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "chargers",
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE chargers SET charge_point_id = 'cp-' || id::text WHERE charge_point_id IS NULL"
        )
    )
    op.alter_column("chargers", "charge_point_id", nullable=False)
    op.create_unique_constraint("uq_chargers_charge_point_id", "chargers", ["charge_point_id"])

    op.create_table(
        "sessions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("charger_id", sa.Integer(), nullable=False),
        sa.Column("connector_id", sa.Integer(), nullable=False),
        sa.Column("id_tag", sa.String(length=255), nullable=False),
        sa.Column("ocpp_transaction_id", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("meter_start", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("meter_stop", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="Active"),
        sa.ForeignKeyConstraint(["charger_id"], ["chargers.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ocpp_transaction_id"),
    )

    op.create_table(
        "meter_values",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=True),
        sa.Column("measurand", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("meter_values")
    op.drop_table("sessions")
    op.drop_constraint("uq_chargers_charge_point_id", "chargers", type_="unique")
    op.drop_column("chargers", "last_heartbeat")
    op.drop_column("chargers", "connector_count")
    op.drop_column("chargers", "charge_point_id")

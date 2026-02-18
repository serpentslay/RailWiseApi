"""raw_service_events unique identity

Revision ID: 523c9c733929
Revises: 66a1e2313f18
Create Date: 2026-02-18 17:23:35.997083

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '523c9c733929'
down_revision: Union[str, Sequence[str], None] = '66a1e2313f18'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

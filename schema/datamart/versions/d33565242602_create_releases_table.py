"""create releases table

Revision ID: d33565242602
Revises: 
Create Date: 2021-03-29 14:58:07.095744

"""
import logging

from alembic import op
import sqlalchemy as sa
from alembic import context
import pandas as pd
from pathlib import Path

# revision identifiers, used by Alembic.
revision = 'd33565242602'
down_revision = None
branch_labels = None
depends_on = None
logger = logging.getLogger(f"alembic.{Path(__file__).name}")


def upgrade():
    op.create_table(
        'release',
        sa.Column('at_date', sa.Date(), nullable=False),
        sa.Column('product_id', sa.String(50), nullable=False),
        sa.Column('version', sa.String(10), nullable=False),
        sa.Column('name', sa.String(10)),
        sa.Column('release_date', sa.Date(), nullable=False),
        sa.Column('link', sa.String(255), nullable=False),
    )
    if context.get_x_argument(as_dictionary=True).get('include-seed-data', None):
        csv_path = Path(__file__).resolve().parent.parent.joinpath('sample_data/releases.csv')
        logger.info(f"Importing data from {csv_path}")
        df_releases = pd.read_csv(csv_path)
        df_releases.to_sql('release', con=op.get_bind(), index=False, if_exists='replace')


def downgrade():
    op.drop_table('release')

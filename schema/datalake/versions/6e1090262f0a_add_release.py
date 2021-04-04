"""add release

Revision ID: 6e1090262f0a
Revises: 
Create Date: 2021-04-02 11:29:40.051831

"""
import logging
import re
from pathlib import Path

from alembic import op, context
import sqlalchemy as sa
import pandas as pd

logger = logging.getLogger(f"alembic.{Path(__file__).name}")

# revision identifiers, used by Alembic.
revision = '6e1090262f0a'
down_revision = None
branch_labels = None
depends_on = None

datalake_fs_location = Path(context.config.get_main_option("datalake_fs_location"))

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
        sample_data = Path(__file__).resolve().parent.parent.joinpath('sample_data/').glob("*.csv")
        for csv_path in sample_data:
            logger.info(f"Importing data from {csv_path}")
            datalake_fs_location.joinpath(csv_path.name).symlink_to(csv_path)
            df_releases = pd.read_csv(csv_path)
            df_releases['at_date'] = re.search(r"_(\d+\-\d+\-\d+)\.", csv_path.name).group(1)
            df_releases.to_sql('release', con=op.get_bind(), index=False, if_exists='append')


def downgrade():
    for csv_path in datalake_fs_location.glob("*.csv"):
        csv_path.unlink()
    op.drop_table('release')


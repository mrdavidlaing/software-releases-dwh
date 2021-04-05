from dagster import pipeline, solid

from src.pipelines.settings_gcp import gcp_mode
from src.pipelines.settings_local_fs import local_fs_mode


@solid
def update_dwh_table(_):
    pass


@pipeline(
    mode_defs=[local_fs_mode, gcp_mode],
)
def populate_dm_pipeline():
    update_dwh_table()

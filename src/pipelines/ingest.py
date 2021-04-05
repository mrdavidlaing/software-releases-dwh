from dagster import pipeline

from src.pipelines.settings_gcp import gcp_mode, gcp_preset
from src.pipelines.settings_inmemory import inmemory_mode, inmemory_preset
from src.pipelines.settings_local_fs import local_fs_mode, local_fs_preset
from src.solids.github import fetch_github_releases, fan_out_fetch_github_releases
from src.solids.releases import add_releases_to_lake, validate_releases, join_releases


@pipeline(
    mode_defs=[inmemory_mode, local_fs_mode, gcp_mode],
    preset_defs=[inmemory_preset, local_fs_preset, gcp_preset]
)
def ingest_pipeline():
    add_releases_to_lake(
        validate_releases(
            join_releases(
                fan_out_fetch_github_releases().map(fetch_github_releases).collect())
        )
    )

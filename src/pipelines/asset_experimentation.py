from dagster import pipeline

from src.pipelines.settings_inmemory import inmemory_mode, inmemory_preset
from src.pipelines.settings_local_fs import local_fs_mode, local_fs_preset
from src.solids.releases import make_asset


@pipeline(
    mode_defs=[inmemory_mode, local_fs_mode],
    preset_defs=[inmemory_preset, local_fs_preset]
)
def asset_experimentation():
    make_asset()
from os.path import abspath
from pathlib import Path

from dagster import ModeDefinition, fs_io_manager, PresetDefinition, file_relative_path
from dagster_ge.factory import ge_data_context

from src.resources.datawarehouse_fs import fs_datawarehouse_resource
from src.resources.github import github_resource

local_fs_mode = ModeDefinition(
    name="local_fs",
    resource_defs={
        "datawarehouse": fs_datawarehouse_resource,
        "github": github_resource,
        "ge_data_context": ge_data_context,
        'io_manager': fs_io_manager
    },
)
local_fs_preset = PresetDefinition(
    name="local_fs",
    mode="local_fs",
    run_config={
        "resources": {
            "datawarehouse": {
                "config": {
                    "base_path": abspath(file_relative_path(__file__, '../../tmp/datawarehouse')),
                    "datalake_schema": "software_releases_lake",
                    "datamart_schema": "software_releases_dm"
                }
            },
            "github": {
                "config": {
                    "github_access_token": {"env": "GITHUB_TOKEN"}
                }
            },
            "ge_data_context": {
                "config": {
                    "ge_root_dir":  abspath(file_relative_path(__file__, "../great_expectations"))
                }
            }
        },
        "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
        "loggers": {"console": {"config": {"log_level": "INFO"}}},
    },
)
from os.path import abspath

from dagster import ModeDefinition, mem_io_manager, file_relative_path, PresetDefinition
from dagster_ge.factory import ge_data_context

from src.resources.datawarehouse_inmemory import inmemory_datawarehouse_resource
from src.resources.github import inmemory_github_resource

inmemory_mode = ModeDefinition(
    name="inmemory",
    resource_defs={
        "datawarehouse": inmemory_datawarehouse_resource,
        "github": inmemory_github_resource,
        "ge_data_context": ge_data_context,
        'io_manager': mem_io_manager
    },
)
inmemory_run_config = {
    "resources": {
        "datawarehouse": {
            "config": {
                "datalake_schema": "software_releases_lake",
                "datamart_schema": "software_releases_dm"
            }
        },
        "ge_data_context": {
            "config": {
                "ge_root_dir": abspath(file_relative_path(__file__, "../great_expectations"))
            }
        },
    },
    "execution": {"in_process": {"config": {"retries": {"disabled": None}}}},
    "loggers": {"console": {"config": {"log_level": "INFO"}}},
}
inmemory_preset = PresetDefinition(
    name="inmemory",
    mode="inmemory",
    run_config=inmemory_run_config,
)
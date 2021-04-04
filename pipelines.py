from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured, default_executors, fs_io_manager, mem_io_manager
from dagster.utils import file_relative_path
from dagster_gcp import gcs_resource
from dagster_gcp.gcs import gcs_plus_default_intermediate_storage_defs, gcs_pickle_io_manager
from dagster_ge.factory import ge_data_context

from github import fetch_github_releases
from releases import add_releases_to_lake, validate_releases, join_releases, make_asset
from resources.datawarehouse_fs import fs_datawarehouse_resource
from resources.datawarehouse_gcp import gcp_datawarehouse_resource
from resources.datawarehouse_inmemory import inmemory_datawarehouse_resource
from resources.github import inmemory_github_resource, github_resource

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
                "ge_root_dir": file_relative_path(__file__, "great_expectations")
            }
        }
    },
    "execution": {"in_process": {"config": {"retries": {"disabled": None}}}},
    "loggers": {"console": {"config": {"log_level": "INFO"}}},
}
inmemory_preset = PresetDefinition(
    name="inmemory",
    mode="inmemory",
    run_config=inmemory_run_config,
)

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
                    "base_path": file_relative_path(__file__, 'tmp/datawarehouse'),
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
                    "ge_root_dir": file_relative_path(__file__, "great_expectations")
                }
            }
        },
        "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
        "loggers": {"console": {"config": {"log_level": "INFO"}}},
    },
)

gcp_mode = ModeDefinition(
    name="gcp",
    intermediate_storage_defs=gcs_plus_default_intermediate_storage_defs,
    resource_defs={
        "datawarehouse": gcp_datawarehouse_resource,
        "github": github_resource,
        "ge_data_context": ge_data_context,
        'gcs': gcs_resource,
        'io_manager': gcs_pickle_io_manager
    },
    executor_defs=default_executors,
)

gcp_preset = PresetDefinition(
    name="gcp",
    mode="gcp",
    run_config={
        "resources": {
            "io_manager": {
                "config": {
                    "gcs_bucket": "software_releases_datalake",
                    "gcs_prefix": "dagster-job"
                }
            },
            "datawarehouse": {
                "config": {
                    "project": "mrdavidlaing",
                    "bucket": "software_releases_datalake",
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
                    "ge_root_dir": file_relative_path(__file__, "great_expectations")
                }
            }
        },
        "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
        "loggers": {"console": {"config": {"log_level": "INFO"}}},
    },
)


@solid
def update_dwh_table(_):
    pass


@configured(fetch_github_releases)
def fetch_kubernetes_releases(_):
    return {'owner': 'kubernetes', "repo": "kubernetes"}


@configured(fetch_github_releases)
def fetch_dagster_releases(_):
    return {'owner': 'dagster-io', "repo": "dagster"}


@configured(fetch_github_releases)
def fetch_knative_serving_releases(_):
    return {'owner': 'knative', "repo": "serving"}


@configured(fetch_github_releases)
def fetch_knative_eventing_releases(_):
    return {'owner': 'knative', "repo": "eventing"}


@configured(fetch_github_releases)
def fetch_cloudfoundry_cf_deployment_releases(_):
    return {'owner': 'cloudfoundry', "repo": "cf-deployment"}


@configured(fetch_github_releases)
def fetch_cloudfoundry_cf_for_k8s_releases(_):
    return {'owner': 'cloudfoundry', "repo": "cf-for-k8s"}


@pipeline(
    mode_defs=[inmemory_mode, local_fs_mode, gcp_mode],
    preset_defs=[inmemory_preset, local_fs_preset, gcp_preset]
)
def ingest_pipeline():
    add_releases_to_lake(
        validate_releases(
            join_releases([
                fetch_kubernetes_releases(),
                fetch_dagster_releases(),
                fetch_knative_eventing_releases(),
                fetch_knative_serving_releases(),
                fetch_cloudfoundry_cf_deployment_releases(),
                fetch_cloudfoundry_cf_for_k8s_releases(),
            ])
        )
    )


@pipeline(
    mode_defs=[inmemory_mode, local_fs_mode],
    preset_defs=[inmemory_preset, local_fs_preset]
)
def asset_experimentation():
    make_asset()


@pipeline(
    mode_defs=[local_fs_mode, gcp_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline, asset_experimentation]

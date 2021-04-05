from os.path import abspath

from dagster import ModeDefinition, default_executors, PresetDefinition, file_relative_path
from dagster_gcp import gcs_resource
from dagster_gcp.gcs import gcs_plus_default_intermediate_storage_defs, gcs_pickle_io_manager
from dagster_ge.factory import ge_data_context

from src.resources.datawarehouse_gcp import gcp_datawarehouse_resource
from src.resources.github import github_resource

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
                    "ge_root_dir": abspath(file_relative_path(__file__, "../great_expectations"))
                }
            }
        },
        "solids": {
            "fan_out_fetch_github_releases": {
                "config": {
                    "repos": [
                        "kubernetes/kubernetes",
                        "dagster-io/dagster",
                        "knative/serving",
                        "cloudfoundry/cf-deployment",
                        "cloudfoundry/cf-for-k8s"
                    ]
                }
            }
        },
        "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
        "loggers": {"console": {"config": {"log_level": "INFO"}}},
    },
)
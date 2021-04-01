import inspect

from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured, default_executors, schedule, fs_io_manager
from dagster.utils import file_relative_path
from dagster_gcp import gcs_resource
from dagster_gcp.gcs import gcs_plus_default_intermediate_storage_defs, gcs_pickle_io_manager
from dagster_ge.factory import ge_data_context

from github import fetch_github_releases
from releases import add_releases_to_lake, validate_releases, join_releases
from resources.datalake import fs_datalake_resource, gcs_datalake_resource
from resources.datamart import postgres_datamart_resource, bigquery_datamart_resource

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "datalake": fs_datalake_resource,
        "datamart": postgres_datamart_resource,
        "ge_data_context": ge_data_context,
        'io_manager': fs_io_manager
    },
)

cloudrun_mode = ModeDefinition(
    name="cloudrun",
    intermediate_storage_defs=gcs_plus_default_intermediate_storage_defs,
    resource_defs={
        "datalake": gcs_datalake_resource,
        "datamart": bigquery_datamart_resource,
        "ge_data_context": ge_data_context,
        'gcs': gcs_resource,
        'io_manager': gcs_pickle_io_manager
    },
    executor_defs=default_executors,
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
    mode_defs=[local_mode, cloudrun_mode],
    preset_defs=[
        PresetDefinition(
            name="default",
            mode="local",
            run_config={
                "resources": {
                    "datamart": {
                        "config": {
                            "hostname": "localhost",
                            "username": "dagster",
                            "password": "dagster",
                            "db_name": "test"
                        }
                    },
                    "datalake": {
                        "config": {
                            "base_path": file_relative_path(__file__, 'tmp/datalake'),
                        }
                    },
                    "ge_data_context": {
                        "config": {
                            "ge_root_dir": file_relative_path(__file__, "great_expectations")
                        }
                    }
                },
                "execution": {"multiprocess": {"config": {"max_concurrent": 1}}},  # For simpler debugging
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
            },
        ),
        PresetDefinition(
            name="GCP",
            mode="cloudrun",
            run_config={
                "resources": {
                    "io_manager": {
                        "config": {
                            "gcs_bucket": "software_releases_datalake",
                            "gcs_prefix": "dagster-job"
                        }
                    },
                    "datalake": {
                        "config": {
                            "bucket": "software_releases_datalake",
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
    ],
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
    mode_defs=[local_mode, cloudrun_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline]

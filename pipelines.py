from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured
from dagster_ge.factory import ge_data_context
from dagster.utils import file_relative_path

from github import fetch_github_releases
from releases import add_releases_to_lake, validate_releases, join_releases
from resources.datamart import postgres_datamart_resource, bigquery_datamart_resource
from resources.datalake import fs_datalake_resource, gcs_datalake_resource

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "datalake": fs_datalake_resource,
        "database": postgres_datamart_resource,
        "ge_data_context": ge_data_context,
    },
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "datalake": gcs_datalake_resource,
        "database": impala_db_resource,
        "ge_data_context": ge_data_context,
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


@pipeline(
    mode_defs=[local_mode, prod_mode],
    preset_defs=[
        PresetDefinition(
            name="default",
            mode="local",
            run_config={
                "resources": {
                    "database": {
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
                "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
                "storage": {"filesystem": {}},
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
            ])
        )
    )


@pipeline(
    mode_defs=[local_mode, prod_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline]

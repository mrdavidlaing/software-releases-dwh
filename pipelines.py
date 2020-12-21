from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured
from dagster_ge.factory import ge_data_context
from dagster.utils import file_relative_path

from database_resources import postgres_db_resource, impala_db_resource
from github import fetch_github_releases
from releases import persist_releases_to_database, join_releases, validate_releases

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "database": postgres_db_resource,
        "ge_data_context": ge_data_context,
    },
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
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
    persist_releases_to_database(
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

from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured

from database import make_sql_solid
from database_resources import postgres_db_resource, impala_db_resource
from github import fetch_github_releases
from releases import load_releases_into_database

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "database": postgres_db_resource,
    },
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "database": impala_db_resource,
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


@pipeline(
    mode_defs=[local_mode, prod_mode],
    preset_defs=[
        PresetDefinition(
            name="default",
            mode="local",
            run_config={
                "resources": {"database": {
                    "config": {"hostname": "localhost", "username": "dagster", "password": "dagster",
                               "db_name": "test"}}},
                "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
                "storage": {"filesystem": {}},
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
            },
        )
    ],
)
def ingest_pipeline():
    # Construct solids
    truncate_releases = make_sql_solid("truncate_releases_table", "TRUNCATE software_releases_lake.releases")
    load_kubernetes_releases = load_releases_into_database.alias("load_kubernetes_releases")
    load_dagster_releases = load_releases_into_database.alias("load_dagster_releases")

    # Construct pipeline
    releases_truncated = truncate_releases()
    load_kubernetes_releases(
        releases=fetch_kubernetes_releases(ok_to_start=releases_truncated)
    )
    load_dagster_releases(
        releases=fetch_dagster_releases(ok_to_start=releases_truncated)
    )


@pipeline(
    mode_defs=[local_mode, prod_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline]

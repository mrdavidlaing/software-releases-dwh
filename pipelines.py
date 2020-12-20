from dagster import pipeline, repository, solid, ModeDefinition

from database_resources import postgres_db_resource, impala_db_resource
from github import make_fetch_github_releases_solid
from releases import load_releases_to_database

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


@pipeline(
    mode_defs=[local_mode, prod_mode],
)
def ingest_pipeline():
    load_releases_to_database.alias("load_kubernetes_release_to_database")(
        releases=make_fetch_github_releases_solid('kubernetes', 'kubernetes', 'kubernetes')()
    )
    load_releases_to_database.alias("load_dagster_release_to_database")(
        releases=make_fetch_github_releases_solid('dagster', 'dagster-io', 'dagster')()
    )


@pipeline(
    mode_defs=[local_mode, prod_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline]

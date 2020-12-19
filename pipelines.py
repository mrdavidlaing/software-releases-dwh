from dagster import pipeline, repository, solid

from github import make_fetch_github_releases_solid


@solid
def update_dwh_table(_):
    pass


@pipeline
def ingest_pipeline():
    make_fetch_github_releases_solid('fetch_kubernetes_releases_from_github', 'kubernetes', 'kubernetes')()
    make_fetch_github_releases_solid('fetch_dagster_releases_from_github', 'dagster-io', 'dagster')()


@pipeline
def populate_dwh_pipeline():
    update_dwh_table()


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline]

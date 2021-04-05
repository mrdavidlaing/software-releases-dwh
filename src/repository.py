from dagster import repository

from src.pipelines.ingest import ingest_pipeline
from src.pipelines.populate_dm import populate_dm_pipeline
from src.pipelines.asset_experimentation import asset_experimentation


@repository
def software_releases_repository():
    """
    The repository definition for this newp Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """

    pipelines = [ingest_pipeline, populate_dm_pipeline, asset_experimentation]
    schedules = []  # [my_hourly_schedule]
    sensors = []  # [my_sensor]

    return pipelines + schedules + sensors

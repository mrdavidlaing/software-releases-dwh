import datetime

import pandas
from dagster import solid, pipeline, DagsterInstance, execute_pipeline

from src.pipelines.settings_inmemory import inmemory_mode, inmemory_preset


@solid(required_resource_keys={"datawarehouse"})
def add_releases_to_lake(context) -> datetime.date:
    df = pandas.DataFrame.from_dict({
        'product_id': ['KEY001', 'KEY2', 'KEY3'],
        'version': ['1.0.0', '2.0.0', '3.0.0'],
        'name': ['v1', 'v2', 'v3'],
        'release_date': [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2), datetime.date(2020, 1, 3)],
        'link': ['http://example.org/1', 'http://example.org/2', 'http://example.org/3']
    })

    asset_key = "release"
    partition_date = datetime.date(2020, 2, 1)

    context.resources.datawarehouse.add_to_lake(df, asset_key, partition_date)

    return partition_date


@solid(required_resource_keys={"datawarehouse"})
def fetch_releases_from_lake(context, partition_date):
    return context.resources.datawarehouse.execute_sql(
        "SELECT * FROM software_releases_lake.release WHERE at_date = ?", (partition_date,)
    )


@pipeline(
    mode_defs=[inmemory_mode],
    preset_defs=[inmemory_preset]
)
def add_and_retrieve_pipeline():
    fetch_releases_from_lake(
        partition_date=add_releases_to_lake()
    )


def test_fs_datalake_resource():
    result = execute_pipeline(
        pipeline=add_and_retrieve_pipeline,
        preset="inmemory",
        instance=DagsterInstance.get(),
    )

    assert result.success


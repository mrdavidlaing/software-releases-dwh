import os
from google.cloud import storage
from collections import namedtuple

from dagster import Field, StringSource, resource
from pandas import DataFrame

DatalakeInfo = namedtuple("DatalakeInfo", "uri add")


@resource(
    {
        "base_path": Field(StringSource),
    }
)
def fs_datalake_resource(init_context):
    base_path = os.path.abspath(init_context.resource_config["base_path"])

    def _do_add(df: DataFrame, asset_type: str, partition_key: str):
        csv_filename = f"{asset_type}_{partition_key}.csv"
        csv_path = os.path.join(base_path, csv_filename)
        df.to_csv(csv_path, index=False)

        return csv_path

    return DatalakeInfo(
        uri=base_path,
        add=_do_add,
    )


@resource(
    {
        "bucket": Field(StringSource),
    }
)
def gcs_datalake_resource(init_context):
    client = storage.Client()
    bucket = client.get_bucket(init_context.resource_config["bucket"])

    def _do_add(df: DataFrame, asset_type: str, partition_key: str):
        csv_filename = f"{asset_type}/partition_key={partition_key}/{asset_type}_{partition_key}.csv"
        bucket.blob(csv_filename).upload_from_string(df.to_csv(index=False), 'text/csv')

        return f"gs://{bucket.name}/{csv_filename}"

    return DatalakeInfo(
        uri=f"gs://{bucket.name}",
        add=_do_add,
    )

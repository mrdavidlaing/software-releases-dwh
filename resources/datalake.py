import os
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

    def _do_add(df: DataFrame, asset_key: str, partition_key: str):
        csv_filename = f"{asset_key}_{partition_key}.csv"
        csv_path = os.path.join(base_path, csv_filename)
        df.to_csv(csv_path, index=False)

        return csv_filename

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
    bucket = os.path.abspath(init_context.resource_config["bucket"])

    def _do_add(df: DataFrame, asset_key: str, partition_key: str):
        raise NotImplemented

        csv_filename = f"{asset_key}_{partition_key}.csv"
        csv_path = os.path.join(bucket, csv_filename)
        df.to_csv(csv_path, index=False)

        return csv_filename

    return DatalakeInfo(
        uri=bucket,
        add=_do_add,
    )

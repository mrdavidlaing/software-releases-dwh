from datetime import datetime, timezone

import pandas
from dagster import solid, OutputDefinition, String, AssetMaterialization, EventMetadataEntry, Output, \
    InputDefinition, Field, Dict, composite_solid, List, AssetKey
from dagster_ge import ge_validation_solid_factory
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

ReleasesDataFrame = create_dagster_pandas_dataframe_type(
    name="ReleasesDataFrame",
    columns=[
        PandasColumn.string_column("product_id", non_nullable=True, is_required=True),
        PandasColumn.string_column("version", non_nullable=True, is_required=True),
        PandasColumn.string_column("name", non_nullable=True, is_required=True),
        PandasColumn.datetime_column("release_date", non_nullable=True, is_required=True),
        PandasColumn.string_column("link", non_nullable=True, is_required=True),
    ],
)


@solid()
def join_releases(_, release_list: List[ReleasesDataFrame]) -> ReleasesDataFrame:
    return pandas.concat(release_list)


@solid(
    config_schema={
        "releases_asset_key": Field(str, is_required=False, default_value="releases")
    },
    description="Persists releases to database (overwriting any existing data)",
    input_defs=[
        InputDefinition(
            name="releases", dagster_type=ReleasesDataFrame,
            description="Releases to persist"),
        InputDefinition(
            name="partition_key", dagster_type=String, default_value="NOW_UTC",
            description="Defaults to current UTC timestamp in YYYYMMDDHHMMSS format"),
    ],
    output_defs=[OutputDefinition(name="asset_path", dagster_type=String)],
    required_resource_keys={"datalake"},
    tags={"kind": "add_to_lake"},
)
def add_releases_to_lake(context, releases, partition_key="NOW_UTC"):
    datalake_uri = context.resources.datalake.uri
    asset_type = "releases"
    if partition_key == "NOW_UTC":
        partition_key = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    asset_path = context.resources.datalake.add(releases, asset_type, partition_key)

    yield AssetMaterialization(
        asset_key=AssetKey(["software_releases_datalake", datalake_uri, asset_type]),
        metadata_entries=[
            EventMetadataEntry.text(datalake_uri, "datalake_uri"),
            EventMetadataEntry.text(asset_type, "asset_type"),
            EventMetadataEntry.text(partition_key, "partition_key"),
            EventMetadataEntry.text(asset_path, "asset_path"),
            EventMetadataEntry.int(releases.shape[0], "rows"),
        ],
    )
    yield Output(value=asset_path, output_name="asset_path")


ge_releases_validation = ge_validation_solid_factory(
    name="validate_releases_expectations",
    datasource_name="dagster_datasource",
    suite_name="software_releases_lake.releases",
    validation_operator_name="action_list_operator",
    input_dagster_type=ReleasesDataFrame,
)


@solid(
    tags={"kind": "raise_on_failure"},
)
def raise_on_failure(_, releases: ReleasesDataFrame, expectation_result: Dict) -> ReleasesDataFrame:
    if expectation_result["success"]:
        return releases
    else:
        raise ValueError


@composite_solid()
def validate_releases(releases: ReleasesDataFrame) -> ReleasesDataFrame:
    return raise_on_failure(releases, ge_releases_validation(releases))

import pandas
from dagster import solid, OutputDefinition, String, AssetMaterialization, EventMetadataEntry, Output, InputDefinition, \
    Field, Dict, composite_solid, List
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
        "releases_table": Field(str, is_required=False, default_value="software_releases_lake.releases")
    },
    description="Persists releases to database (overwriting any existing data)",
    output_defs=[OutputDefinition(name="releases_table", dagster_type=String)],
    required_resource_keys={"database"},
    tags={"kind": "load_into_database"},
)
def persist_releases_to_database(context, releases: ReleasesDataFrame):
    releases_table = context.solid_config["releases_table"]
    context.resources.database.load_table(releases, releases_table, overwrite=True)

    yield AssetMaterialization(
        asset_key=f"table:{releases_table}",
        description=f"Persisted table {releases_table} in database configured in the database resource.",
        metadata_entries=[
            EventMetadataEntry.text(label="host", text=context.resources.database.host),
            EventMetadataEntry.text(label="db_name", text=context.resources.database.db_name),
            EventMetadataEntry.text(label="releases_table", text=releases_table),
        ],
    )
    yield Output(value=releases_table, output_name="releases_table")


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

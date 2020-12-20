from dagster import solid, OutputDefinition, String, AssetMaterialization, EventMetadataEntry, Output, InputDefinition, \
    Field
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


@solid(
    config_schema={
        "releases_table": Field(str, is_required=False, default_value="releases")
    },
    output_defs=[OutputDefinition(name="releases_table", dagster_type=String)],
    required_resource_keys={"database"},
)
def load_releases_to_database(context, releases: ReleasesDataFrame):
    releases_table = context.solid_config["releases_table"]
    context.resources.database.load_table(releases, releases_table)

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


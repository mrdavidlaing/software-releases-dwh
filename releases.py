from dagster import solid, OutputDefinition, String, AssetMaterialization, EventMetadataEntry, Output
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

ReleasesDataFrame = create_dagster_pandas_dataframe_type(
    name="ReleasesDataFrame",
    columns=[
        PandasColumn.string_column("version"),
        PandasColumn.string_column("name"),
        PandasColumn.datetime_column("release_date"),
        PandasColumn.string_column("link"),
    ],
)


# @solid(
#     output_defs=[OutputDefinition(name="table_name", dagster_type=String)],
#     config_schema={"table_name": String},
#     required_resource_keys={"db_info"},
# )
# def load_releases_to_database(context, releases: ReleasesDataFrame):
#     context.resources.db_info.load_table(releases, context.solid_config["table_name"])
#
#     table_name = context.solid_config["table_name"]
#     yield AssetMaterialization(
#         asset_key="table:{table_name}".format(table_name=table_name),
#         description=(
#             "Persisted table {table_name} in database configured in the db_info resource."
#         ).format(table_name=table_name),
#         metadata_entries=[
#             EventMetadataEntry.text(label="Host", text=context.resources.db_info.host),
#             EventMetadataEntry.text(label="Db", text=context.resources.db_info.db_name),
#         ],
#     )
#     yield Output(value=table_name, output_name="table_name")

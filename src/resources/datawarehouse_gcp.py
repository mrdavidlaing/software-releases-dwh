from datetime import datetime

from dagster import resource, Field, StringSource
from dagster_pandas import DataFrame
from google.cloud import storage

from src.resources.datawarehouse import DatawarehouseInfo


@resource(
    {
       "project": Field(StringSource, description="Project ID for the project which the client acts on behalf of"),
       "bucket": Field(StringSource),
       "datalake_schema": Field(StringSource),
       "datamart_schema": Field(StringSource),
    }
)
def gcp_datawarehouse_resource(init_context):
    client = storage.Client()
    bucket = client.get_bucket(init_context.resource_config["bucket"])

    def _execute_sql(query: str, params: tuple = None):
        raise NotImplemented()
        # return pandas.read_sql_query(query, _dwh.connection, params=params)

    def _do_add_to_lake(df: DataFrame, asset_type: str, partition_key: str):
        csv_filename = f"{asset_type}/partition_key={partition_key}/{asset_type}_{partition_key}.csv"
        bucket.blob(csv_filename).upload_from_string(df.to_csv(index=False), 'text/csv')

        return f"gs://{bucket.name}/{csv_filename}"

    def _replace_partition(df: DataFrame, schema: str, table: str, at_date: datetime) -> str:
        raise NotImplemented()
        # df['at_date'] = at_date
        #
        # buffer = StringIO()
        # df.to_csv(buffer, index=False, header=False)
        # buffer.seek(0)
        #
        # fq_table_name = f"{schema}.{table}"
        # _dwh.connection.execute(f"DELETE FROM {fq_table_name} WHERE at_date=%s", at_date)
        # df.to_sql(fq_table_name, con=_dwh.connection, index=False, if_exists='append')
        # return f"{fq_table_name} WHERE at_date={at_date}"

    # TODO: init gcp connection
    connection = None
    yield DatawarehouseInfo(
        datalake_uri=f"gs://{bucket.name}",
        datalake_schema=init_context.resource_config["datalake_schema"],
        datamart_schema=init_context.resource_config["datamart_schema"],
        connection=connection,
        add_to_lake=_do_add_to_lake,
        replace_partition=_replace_partition,
        execute_sql=_execute_sql,
    )

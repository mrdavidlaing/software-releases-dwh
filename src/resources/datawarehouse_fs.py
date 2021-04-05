import os
from datetime import date
from io import StringIO

import pandas
from dagster import resource, Field, StringSource
from dagster_pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.pool import SingletonThreadPool

from src.resources.datawarehouse import DatawarehouseInfo


@resource(
    {
        "base_path": Field(StringSource),
        "datalake_schema": Field(StringSource),
        "datamart_schema": Field(StringSource),
    }
)
def fs_datawarehouse_resource(init_context):
    _base_path = os.path.abspath(init_context.resource_config["base_path"])
    _datalake_schema = init_context.resource_config["datalake_schema"]
    _datamart_schema = init_context.resource_config["datamart_schema"]
    _connection_initialised = False

    engine = create_engine('sqlite://', echo=True, poolclass=SingletonThreadPool)
    with engine.begin() as connection:
        if not _connection_initialised:
            engine.execute(f"ATTACH DATABASE 'file:{_base_path}/datalake.db' as {_datalake_schema};")
            engine.execute(f"ATTACH DATABASE 'file:{_base_path}/datamart.db' as {_datamart_schema};")
            _connection_initialised = True

        def _execute_sql(query: str, params: tuple = None):
            return pandas.read_sql_query(query, connection, params=params)

        def _do_add_to_lake(df: DataFrame, asset_type: str, at_date: date):
            df['at_date'] = at_date

            csv_filename = f"{asset_type}_{at_date.strftime('%Y-%m-%d')}.csv"
            csv_path = os.path.join(_base_path, csv_filename)
            df.to_csv(csv_path, index=False)

            connection.execute(f"DELETE FROM {_datalake_schema}.{asset_type} WHERE at_date=?", at_date)
            df.to_sql(asset_type, schema=_datalake_schema, con=connection, index=False, if_exists='append')
            return csv_path

        def _replace_partition(df: DataFrame, schema: str, table: str, at_date: date) -> str:
            df['at_date'] = at_date

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            fq_table_name = f"{schema}.{table}"
            connection.execute(f"DELETE FROM {fq_table_name} WHERE at_date=?", at_date)
            df.to_sql(table, schema=schema, con=connection, index=False, if_exists='append')
            return f"{fq_table_name} WHERE at_date={at_date}"

        yield DatawarehouseInfo(
            datalake_uri=f"file://{_base_path}",
            datalake_schema=_datalake_schema,
            datamart_schema=_datamart_schema,
            connection=connection,
            add_to_lake=_do_add_to_lake,
            replace_partition=_replace_partition,
            execute_sql=_execute_sql,
        )

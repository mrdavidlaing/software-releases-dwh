import os
from contextlib import contextmanager
from datetime import date
from io import StringIO
from pathlib import Path

import pandas
from alembic import command, config
from dagster import resource, Field, StringSource
from dagster_pandas import DataFrame
from sqlalchemy import create_engine, event
from sqlalchemy.pool import SingletonThreadPool, StaticPool, NullPool

from resources.datawarehouse import DatawarehouseInfo


@contextmanager
def pushd(path):
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def run_alembic_migration(connection, database_name, database_schema):
    with pushd(Path(__file__).resolve().parent.parent):
        cfg = config.Config(
            'alembic.ini',
            ini_section=database_name,
            cmd_opts=config.CommandLine().parser.parse_args([
                # "-x", "include-seed-data=true",
            ]),
        )
        cfg.attributes['connection'] = connection.execution_options(schema_translate_map={None: database_schema})
        connection.dialect.default_schema_name = database_name
        command.upgrade(cfg, "head")


@resource(
    {
        "datalake_schema": Field(StringSource),
        "datamart_schema": Field(StringSource),
    }
)
def inmemory_datawarehouse_resource(init_context):
    _datalake_schema = init_context.resource_config["datalake_schema"]
    _datamart_schema = init_context.resource_config["datamart_schema"]
    _connection_initialised = False

    engine = create_engine('sqlite://', echo=True, poolclass=SingletonThreadPool)
    with engine.begin() as connection:
        if not _connection_initialised:
            engine.execute(f"ATTACH DATABASE 'file:inmemory_datalake?mode=memory' as {_datalake_schema};")
            engine.execute(f"ATTACH DATABASE 'file:inmemory_datamart?mode=memory' as {_datamart_schema};")
            run_alembic_migration(connection, "datalake", _datalake_schema)
            run_alembic_migration(connection, "datamart", _datamart_schema)
            _connection_initialised = True

        def _execute_sql(query: str, params: tuple = None):
            return pandas.read_sql_query(query, connection, params=params)

        def _do_add_to_lake(df: DataFrame, asset_type: str, at_date: date):
            df['at_date'] = at_date

            csv_path = f"/not/storing/at/base_path/{asset_type}_{at_date.strftime('%Y-%m-%d')}.csv"

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
            datalake_uri=f":inmemory:",
            datalake_schema=_datalake_schema,
            datamart_schema=_datamart_schema,
            connection=connection,
            add_to_lake=_do_add_to_lake,
            replace_partition=_replace_partition,
            execute_sql=_execute_sql,
        )

import psycopg2
from io import StringIO
from collections import namedtuple
from urllib.parse import urlparse

import sqlalchemy
from dagster import Field, IntSource, StringSource, resource, Output

DatabaseInfo = namedtuple("DatabaseInfo", "connection execute_sql load_table host db_name")


# def create_impala_db_url(username, password, hostname, port, db_name, jdbc=True):
#     if jdbc:
#         db_url = (
#             "jdbc:postgresql://{hostname}:{port}/{db_name}?"
#             "user={username}&password={password}".format(
#                 username=username, password=password, hostname=hostname, port=port, db_name=db_name
#             )
#         )
#     else:
#         db_url = "redshift+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}".format(
#             username=username, password=password, hostname=hostname, port=port, db_name=db_name
#         )
#     return db_url


# def create_impala_engine(db_url):
#     return sqlalchemy.create_engine(db_url)
#
@resource(
    {
        "username": Field(StringSource),
        "password": Field(StringSource),
        "hostname": Field(StringSource),
        "port": Field(IntSource, is_required=False, default_value=5439),
        "db_name": Field(StringSource),
    }
)
def impala_db_resource(init_context):
    raise NotImplemented("TODO: Implement Impala DB resource")


#     host = init_context.resource_config["hostname"]
#     db_name = init_context.resource_config["db_name"]
#
#     db_url_jdbc = create_impala_db_url(
#         username=init_context.resource_config["username"],
#         password=init_context.resource_config["password"],
#         hostname=host,
#         port=init_context.resource_config["port"],
#         db_name=db_name,
#     )
#
#     db_url = create_impala_db_url(
#         username=init_context.resource_config["username"],
#         password=init_context.resource_config["password"],
#         hostname=host,
#         port=init_context.resource_config["port"],
#         db_name=db_name,
#         jdbc=False,
#     )
#
#     s3_temp_dir = init_context.resource_config["s3_temp_dir"]
#
#     def _do_load(data_frame, table_name):
#         data_frame.write.format("com.databricks.spark.redshift").option(
#             "tempdir", s3_temp_dir
#         ).mode("overwrite").jdbc(db_url_jdbc, table_name)
#
#     return DatabaseInfo(
#         url=db_url,
#         engine=create_impala_engine(db_url),
#         dialect="???",
#         load_table=_do_load,
#         host=host,
#         db_name=db_name,
#     )

def create_postgres_connection(username, password, hostname, port, db_name):
    return psycopg2.connect(
        user=username,
        password=password,
        host=hostname,
        port=port,
        database=db_name,
    )


@resource(
    {
        "username": Field(StringSource),
        "password": Field(StringSource),
        "hostname": Field(StringSource),
        "port": Field(IntSource, is_required=False, default_value=5432),
        "db_name": Field(StringSource),
    }
)
def postgres_db_resource(init_context):
    host = init_context.resource_config["hostname"]
    db_name = init_context.resource_config["db_name"]

    postgres_connection = create_postgres_connection(
        username=init_context.resource_config["username"],
        password=init_context.resource_config["password"],
        hostname=host,
        port=init_context.resource_config["port"],
        db_name=db_name
    )

    def _do_load(df, table_name, overwrite=False):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        with postgres_connection:
            with postgres_connection.cursor() as cursor:
                if overwrite:
                    cursor.execute(f"TRUNCATE {table_name}")
                cursor.copy_from(buffer, table_name, sep=",")

    def _do_execute_sql(sql, data=None):
        with postgres_connection:
            with postgres_connection.cursor() as cursor:
                cursor.execute(sql, data)
                return cursor.statusmessage, cursor.rowcount

    return DatabaseInfo(
        connection=postgres_connection,
        execute_sql=_do_execute_sql,
        load_table=_do_load,
        host=host,
        db_name=db_name,
    )

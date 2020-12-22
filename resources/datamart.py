from collections import namedtuple
from io import StringIO

import psycopg2
from dagster import Field, IntSource, StringSource, resource, Output, InputDefinition, OutputDefinition, check, solid
from dagster.core.types.dagster_type import create_string_type

DatamartInfo = namedtuple("DatabaseInfo", "connection execute_sql load_table host db_name")


@resource(
    {
        'project': Field(
            StringSource, description=
            """Project ID for the project which the client acts on behalf of. Will be passed
               when creating a dataset / job. If not passed, falls back to the default inferred from the
               environment.""",
            is_required=False,
        ),
        'location': Field(
            StringSource,
            description="(Optional) Default location for jobs / datasets / tables.",
            is_required=False,
        )
    }
)
def bigquery_datamart_resource(init_context):
    raise NotImplemented()


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
def postgres_datamart_resource(init_context):
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

    return DatamartInfo(
        connection=postgres_connection,
        execute_sql=_do_execute_sql,
        load_table=_do_load,
        host=host,
        db_name=db_name,
    )


SqlTableName = create_string_type("SqlTableName", description="The name of a database table")


def make_sql_solid(name, select_statement, materialization_strategy=None, table_name=None, input_defs=None):
    """Return a new solid that executes and materializes a SQL select statement.

    Args:
        name (str): The name of the new solid.
        select_statement (str): The select statement to execute.
        materialization_strategy (str): Must be 'table', the only currently supported
            materialization strategy. If 'table', the kwarg `table_name` must also be passed.
        table_name (str): THe name of the new table to create, if the materialization strategy
            is 'table'. Default: None.
        input_defs (list[InputDefinition]): Inputs, if any, for the new solid. Default: None.

    Returns:
        function:
            The new SQL solid.
    """
    input_defs = check.opt_list_param(input_defs, "input_defs", InputDefinition)

    materialization_strategy_output_types = {  # pylint:disable=C0103
        "table_name": SqlTableName,
        # 'view': String,
        # 'query': SqlAlchemyQueryType,
        # 'subquery': SqlAlchemySubqueryType,
        # 'result_proxy': SqlAlchemyResultProxyType,
        # could also materialize as a Pandas table, as a Spark table, as an intermediate file, etc.
    }

    if materialization_strategy and materialization_strategy not in materialization_strategy_output_types:
        raise Exception(
            "Invalid materialization strategy {materialization_strategy}, must "
            "be one of {materialization_strategies}".format(
                materialization_strategy=materialization_strategy,
                materialization_strategies=str(list(materialization_strategy_output_types.keys())),
            )
        )

    if materialization_strategy == "table":
        if table_name is None:
            raise Exception("Missing table_name: required for materialization strategy 'table'")

    output_description = (
        "The string name of the new table created by the solid"
        if materialization_strategy == "table"
        else "The materialized SQL statement. If the materialization_strategy is "
             "'table', this is the string name of the new table created by the solid."
    )

    description = """This solid executes the following SQL statement:
    {select_statement}""".format(
        select_statement=select_statement
    )

    # n.b., we will eventually want to make this resources key configurable
    # sql_statement = (
    #     "drop table if exists {table_name};\n" "create table {table_name} as {select_statement};"
    # ).format(table_name=table_name, select_statement=select_statement)
    sql_statement = select_statement

    @solid(
        name=name,
        input_defs=input_defs,
        output_defs=[
            OutputDefinition(name="status_message", description="Status of SQL query execution"),
            OutputDefinition(name="rowcount", description="The number of rows affected by the SQL query")
        ],
        description=description,
        required_resource_keys={"datamart"},
        tags={"kind": "sql", "sql": sql_statement},
    )
    def _sql_solid(context, **input_defs):  # pylint: disable=unused-argument
        """Inner function defining the new solid.

        Args:
            context (SolidExecutionContext): Must expose a `database` resource with an `execute` method,
                like a SQLAlchemy engine, that can execute raw SQL against a database.

        Returns:
            str:
                The table name of the newly materialized SQL select statement.
        """
        context.log.info(
            "Executing sql statement:\n{sql_statement}".format(sql_statement=sql_statement)
        )
        status_message, rowcount = context.resources.datamart.execute_sql(sql_statement)
        context.log.info(f"Query status: {status_message}, row count:{rowcount}")

        yield Output(value=status_message, output_name="status_message")
        yield Output(value=rowcount, output_name="rowcount")

    return _sql_solid

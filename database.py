from dagster import (
    InputDefinition,
    Output,
    OutputDefinition,
    check,
    solid, ExpectationResult, EventMetadataEntry,
)
from dagster.core.types.dagster_type import create_string_type

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
            OutputDefinition(name="rowcount", description="The number of rows affected by the SQL query")
        ],
        description=description,
        required_resource_keys={"database"},
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
        statusmessage, rowcount = context.resources.database.execute_sql(sql_statement)
        context.log.info(f"Query status: {statusmessage}, row count:{rowcount}")

        yield Output(value=rowcount, output_name="rowcount")

    return _sql_solid

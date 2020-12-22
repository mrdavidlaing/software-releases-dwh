from dagster import execute_solid, ModeDefinition

from resources.datamart import make_sql_solid, postgres_datamart_resource

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "datamart": postgres_datamart_resource,
    },
)
test_run_config = {
    "resources": {
        "datamart": {
            "config": {
                "hostname": "localhost",
                "username": "dagster",
                "password": "dagster",
                "db_name": "test"
            }
        }
    }
}


def test_sql_solid():
    sample_sql_solid = make_sql_solid(
        name="sample_sql_solid",
        select_statement="SELECT * FROM software_releases_dwh.dim_releases LIMIT 5",
    )
    assert sample_sql_solid

    result = execute_solid(sample_sql_solid, mode_def=test_mode, run_config=test_run_config)
    assert result.success
    assert result.output_value("status_message") == "SELECT 0"

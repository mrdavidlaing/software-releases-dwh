import pandas
import pytest
from dagster import execute_solid, solid

from tests.conftest import test_mode, test_run_config


@solid(required_resource_keys={"datalake"})
def example_add_solid(context):
    df = pandas.DataFrame.from_dict({
        'key': ['KEY001', 'KEY2', 'KEY3'],
        'value': [1, 2, 3],
    })
    asset_key = "test-asset"
    partition_key = "partition-one"

    return context.resources.datalake.add(df, asset_key, partition_key)


@pytest.mark.usefixtures("cleanup_datalake")
@pytest.mark.asset_type("test-asset")
def test_fs_datalake_resource():
    result = execute_solid(
        example_add_solid,
        mode_def=test_mode,
        run_config=test_run_config
    )

    assert result.success
    assert 'test-asset_partition-one.csv' in result.output_value()

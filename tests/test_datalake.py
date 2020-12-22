import re

import pandas
from dagster import execute_solid, ModeDefinition, file_relative_path, solid

from resources.datalake import fs_datalake_resource

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "datalake": fs_datalake_resource,
    },
)
test_run_config = {
    "resources": {
        "datalake": {
            "config": {
                "base_path": file_relative_path(__file__, '../tmp/datalake'),
            }
        }
    }
}


@solid(required_resource_keys={"datalake"})
def example_add_solid(context):
    df = pandas.DataFrame.from_dict({
        'key': ['KEY001', 'KEY2', 'KEY3'],
        'value': [1, 2, 3],
    })
    asset_key = "test-asset"
    partition_key = "partition-one"

    return context.resources.datalake.add(df, asset_key, partition_key)


def test_fs_datalake_resource():
    result = execute_solid(
        example_add_solid,
        mode_def=test_mode,
        run_config=test_run_config
    )

    assert result.success
    assert re.match(r'test-asset\_partition-one.csv', result.output_value())

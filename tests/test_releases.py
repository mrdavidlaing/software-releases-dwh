import re
from datetime import datetime

import pandas
from dagster import execute_solid, ModeDefinition, file_relative_path

from releases import add_releases_to_lake
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
    },
    "solids": {
        "add_releases_to_lake": {
            "config": {
                "releases_asset_key": "test-releases"
            }
        }
    }
}


def test_add_releases_to_lake():
    sample_releases = pandas.DataFrame.from_dict({
        'product_id': ['knative/serving', 'knative/serving', 'knative/serving'],
        'version': ['1.0.0', '1.0.1', '1.1.0'],
        'name': ['v1', 'v1 patch 1', 'v1.1'],
        'release_date': [datetime(2020, 1, 2), datetime(2020, 1, 17), datetime(2020, 2, 28)],
        'link': ['https://github.com/knative/serving/releases/1.0.0',
                 'https://github.com/knative/serving/releases/1.0.1',
                 'https://github.com/knative/serving/releases/1.1.0'],
    })

    result = execute_solid(
        add_releases_to_lake,
        input_values={'releases': sample_releases},
        mode_def=test_mode,
        run_config=test_run_config
    )

    assert result.success
    assert re.match(r'test-releases\_\d+\-\d+\.csv', result.output_value('asset_path'))


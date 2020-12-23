import glob
import os

import pytest
from dagster import file_relative_path, ModeDefinition

from resources.datalake import fs_datalake_resource

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "datalake": fs_datalake_resource,
    },
)
test_datalake_path = file_relative_path(__file__, '../tmp/datalake')
test_run_config = {
    "resources": {
        "datalake": {
            "config": {
                "base_path": test_datalake_path,
            }
        }
    }
}


@pytest.fixture()
def cleanup_datalake(request):
    def remove_test_assets():
        asset_type = request.node.get_closest_marker("asset_type").args[0]
        files = glob.glob(os.path.join(test_datalake_path, f'{asset_type}_*.csv'))
        for f in files:
            os.remove(f)

    remove_test_assets()  # Cleanup before tests run
    yield
    remove_test_assets()  # Cleanup after tests run

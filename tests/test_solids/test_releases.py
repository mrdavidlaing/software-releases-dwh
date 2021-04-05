from datetime import datetime, timezone

import pandas
from dagster import execute_solid

from src.pipelines.settings_inmemory import inmemory_mode, inmemory_run_config
from src.solids.releases import add_releases_to_lake


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
        mode_def=inmemory_mode,
        run_config=inmemory_run_config,
    )

    assert result.success
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert f'release_{today}.csv' in result.output_value('asset_path')

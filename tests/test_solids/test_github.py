from dagster import execute_solid, configured

from src.pipelines.settings_inmemory import inmemory_mode, inmemory_run_config
from src.solids.github import fetch_github_releases


def test_fetch_kubernetes_releases():
    @configured(fetch_github_releases)
    def fetch_kubernetes_releases(_):
        return {'owner': 'kubernetes', "repo": "kubernetes"}

    res = execute_solid(
        fetch_github_releases,
        input_values={'owner_repo': "kubernetes/kubernetes"},
        mode_def=inmemory_mode,
        run_config=inmemory_run_config
    )
    assert res.success
    assert len(res.output_value('releases')) >= 3, "kubernetes/kubernetes should have 3 mocked releases"

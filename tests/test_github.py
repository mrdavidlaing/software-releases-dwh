from dagster import execute_solid, configured

from github import fetch_github_releases
from pipelines import inmemory_mode, inmemory_run_config


def test_fetch_kubernetes_releases():
    @configured(fetch_github_releases)
    def fetch_kubernetes_releases(_):
        return {'owner': 'kubernetes', "repo": "kubernetes"}

    res = execute_solid(fetch_kubernetes_releases, mode_def=inmemory_mode, run_config=inmemory_run_config)
    assert res.success
    assert len(res.output_value('releases')) >= 3, "kubernetes/kubernetes should have 3 mocked releases"

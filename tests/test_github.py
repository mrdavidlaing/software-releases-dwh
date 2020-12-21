from dagster import execute_solid, configured

from github import fetch_github_releases


def test_fetch_kubernetes_releases():
    @configured(fetch_github_releases)
    def fetch_kubernetes_releases(_):
        return {'owner': 'kubernetes', "repo": "kubernetes"}

    res = execute_solid(fetch_kubernetes_releases)
    assert res.success
    assert len(res.output_value('releases')) > 100, "kubernetes/kubernetes should have more than 100 releases on GitHub"

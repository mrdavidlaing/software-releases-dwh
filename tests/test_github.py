from dagster import execute_solid

from github import make_fetch_github_releases_solid


def test_fetch_github_releases():
    res = execute_solid(
        make_fetch_github_releases_solid("fetch_kubernetes_releases_from_github", "kubernetes", "kubernetes")
    )
    assert res.success
    assert len(res.output_value('releases')) > 100, "kubernetes/kubernetes should have more than 100 releases on GitHub"

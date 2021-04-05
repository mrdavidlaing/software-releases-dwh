from collections import namedtuple

import ghapi
from dagster import resource, Field, StringSource
from ghapi.core import GhApi
from ghapi.page import pages

GithubInfo = namedtuple(
    "GithubInfo",
    "get_releases"
)


@resource()
def inmemory_github_resource(context):

    def _get_releases(owner: str, repo: str):
        releases = [
            {'tag_name': '1.0.0', 'name': 'v1', 'published_at': "2021-02-28T18:00:01Z", 'html_url':f"https://github.com/{owner}/{repo}/releases/tag/1.0.0", 'draft': False},
            {'tag_name': '1.0.1', 'name': 'v1 patch 1', 'published_at': "2021-03-02T11:45:02Z", 'html_url': f"https://github.com/{owner}/{repo}/releases/tag/1.0.1", 'draft': False},
            {'tag_name': '1.1.0', 'name': 'v1.1', 'published_at': "2021-03-10T14:00:03Z",  'html_url': f"https://github.com/{owner}/{repo}/releases/tag/1.1.0", 'draft': False},
        ]
        context.log.info(
            f"Generated {len(releases)} mock releases for GitHub project: {owner}/{repo}"
        )
        return releases

    yield GithubInfo(
        get_releases=_get_releases
    )


@resource(
    {
        "github_access_token": Field(StringSource)
    }
)
def github_resource(context):
    _api = GhApi(token=context.resource_config["github_access_token"])

    def _get_releases(owner: str, repo: str):
        try:
            releases = _api.repos.list_releases(owner, repo, per_page=100)
            total_api_requests = 1
            if _api.last_page() > 0:
                total_api_requests += _api.last_page()
                releases = pages(_api.repos.list_releases, _api.last_page(), owner, repo, per_page=100).concat()
            context.log.info(
                f"Retrieved {len(releases)} releases for GitHub project: {owner}/{repo}"
                f" ({total_api_requests} API request(s) made, quota remaining: {_api.limit_rem})"
            )
            return releases
        except ghapi.core.HTTP4xxClientError as http_error:
            if 'rate limit exceeded' in http_error.fp.reason.lower():
                raise ConnectionRefusedError(
                    f"Insufficient GitHub API call quota remaining to fetch releases for GitHub project: {owner}/{repo}.\n"
                    f"https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api#rate-limiting"
                )
            else:
                raise http_error

    yield GithubInfo(
        get_releases=_get_releases
    )

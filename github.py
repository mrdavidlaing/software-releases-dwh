import os
import ghapi.core
import pandas
from dagster import solid, Field, OutputDefinition, Output, InputDefinition
from ghapi.page import pages

from releases import ReleasesDataFrame


def make_fetch_github_releases_solid(product_id: str, owner: str, repo: str):
    """Return a new solid that fetches release information from GitHub

    Args:
        product_id (str): The unique Id for the product (also used as the name of the new solid)
        owner (str): The GitHub owner - github.com/<owner>/repo-name
        repo (str): The GitHub repo - github.com/owner-name/<repo>

    Returns:
        function:
            The new fetch_github_releases solid.
    """

    @solid(
        name=f"fetch_{product_id}",
        config_schema={
            "github_access_token": Field(str, is_required=False, default_value=os.getenv('GITHUB_TOKEN', 'undefined'))
        },
        input_defs=[
            InputDefinition(name="ok_to_start", default_value=True,
                            description="Used to chain to other solids.  Value ignored")
        ],
        output_defs=[OutputDefinition(name="releases", dagster_type=ReleasesDataFrame)],
        tags={"kind": "github_releases"},
    )
    def _fetch_github_releases(context, ok_to_start):  # pylint: disable=unused-argument
        if context.solid_config["github_access_token"] == 'undefined':
            raise LookupError("Must have GITHUB_TOKEN environment variable or configure github_access_token")

        api = ghapi.core.GhApi(token=context.solid_config["github_access_token"])
        try:
            releases = pages(api.repos.list_releases, 5, owner, repo).concat()
            # TODO: use github_api.last_page() as below to fetch total pages
            #  - as soon as this BUGFIX has been published https://github.com/fastai/ghapi/commit/5df646198a58920822a9ca21b1bdada1511f7883
            # releases = pages(github_api.repos.list_releases, github_api.last_page(), owner, repo).concat()
            context.log.info(
                f"Retrieved {len(releases)} releases for GitHub project: {owner}/{repo}"
                f" ({api.limit_rem} API calls remaining of quota)"
            )
            releases_df = pandas.DataFrame.from_records(releases, columns=[
                'tag_name', 'name', 'published_at', 'html_url'
            ]).rename(columns={
                'tag_name': 'version',
                'published_at': 'release_date',
                'html_url': 'link'
            })

            releases_df.insert(0, 'product_id', product_id) # Add product_id as the first column
            releases_df.release_date = pandas.to_datetime(releases_df.release_date, infer_datetime_format=True)

            yield Output(releases_df, output_name="releases")
        except ghapi.core.HTTP4xxClientError as http_error:
            if 'rate limit exceeded' in http_error.fp.reason.lower():
                raise ConnectionRefusedError(
                    f"Insufficient GitHub API call quota remaining to fetch releases for GitHub project: {owner}/{repo}.\n"
                    f"https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api#rate-limiting"
                )
            else:
                raise http_error

    return _fetch_github_releases


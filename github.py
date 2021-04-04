import os

import ghapi.core
import pandas
from dagster import solid, Field, OutputDefinition, Output
from ghapi.page import pages

from releases import ReleasesDataFrame


@solid(
    config_schema={
        "owner": Field(str, is_required=True, description="github.com/<owner>/repo-name"),
        "repo": Field(str, is_required=True, description="github.com/owner-name/<repo>"),
    },
    required_resource_keys={"github"},
    output_defs=[OutputDefinition(name="releases", dagster_type=ReleasesDataFrame)],
    tags={"kind": "github_releases"},
)
def fetch_github_releases(context):
    owner = context.solid_config["owner"]
    repo = context.solid_config["repo"]

    releases = context.resources.github.get_releases(owner, repo)

    releases_df = pandas.DataFrame.from_records(releases, columns=[
        'tag_name', 'name', 'published_at', 'html_url', 'draft'
    ]).rename(columns={
        'tag_name': 'version',
        'published_at': 'release_date',
        'html_url': 'link'
    })
    # Ignore DRAFT releases (which don't yet have a release date)
    releases_df = releases_df.drop(releases_df[releases_df['draft'] == True].index).drop(columns=['draft'])

    releases_df.insert(0, 'product_id', f"{owner}/{repo}")  # Derive product_id and make it the first column
    releases_df.release_date = pandas.to_datetime(releases_df.release_date, infer_datetime_format=True)

    yield Output(releases_df, output_name="releases")

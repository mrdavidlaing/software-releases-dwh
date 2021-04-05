import re

import pandas
from dagster import solid, Field, OutputDefinition, Output, InputDefinition
from dagster.core.definitions import DynamicOutput
from dagster.core.definitions.output import DynamicOutputDefinition

from src.solids.releases import ReleasesDataFrame


@solid(
    config_schema={"repos": Field([str], default_value=['kubernetes/kubernetes', 'dagster-io/dagster'])},
    output_defs=[DynamicOutputDefinition(str)],
    tags={"kind": "github_releases"},
)
def fan_out_fetch_github_releases(context):
    repos = context.solid_config["repos"]
    for repo in repos:
        yield DynamicOutput(value=repo, mapping_key=re.sub(r'[-/.]', '_', repo))


@solid(
    input_defs=[
        InputDefinition(name="owner_repo", dagster_type=str, description="github.com/<owner/repo>"),
    ],
    required_resource_keys={"github"},
    output_defs=[OutputDefinition(name="releases", dagster_type=ReleasesDataFrame)],
    tags={"kind": "github_releases"},
)
def fetch_github_releases(context, owner_repo: str):
    owner, repo = owner_repo.split("/")

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
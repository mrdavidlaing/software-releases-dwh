from dagster import pipeline, configured

from src.pipelines.settings_gcp import gcp_mode, gcp_preset
from src.pipelines.settings_inmemory import inmemory_mode, inmemory_preset
from src.pipelines.settings_local_fs import local_fs_mode, local_fs_preset
from src.solids.github import fetch_github_releases
from src.solids.releases import add_releases_to_lake, validate_releases, join_releases


@configured(fetch_github_releases)
def fetch_kubernetes_releases(_):
    return {'owner': 'kubernetes', "repo": "kubernetes"}


@configured(fetch_github_releases)
def fetch_dagster_releases(_):
    return {'owner': 'dagster-io', "repo": "dagster"}


@configured(fetch_github_releases)
def fetch_knative_serving_releases(_):
    return {'owner': 'knative', "repo": "serving"}


@configured(fetch_github_releases)
def fetch_knative_eventing_releases(_):
    return {'owner': 'knative', "repo": "eventing"}


@configured(fetch_github_releases)
def fetch_cloudfoundry_cf_deployment_releases(_):
    return {'owner': 'cloudfoundry', "repo": "cf-deployment"}


@configured(fetch_github_releases)
def fetch_cloudfoundry_cf_for_k8s_releases(_):
    return {'owner': 'cloudfoundry', "repo": "cf-for-k8s"}


@pipeline(
    mode_defs=[inmemory_mode, local_fs_mode, gcp_mode],
    preset_defs=[inmemory_preset, local_fs_preset, gcp_preset]
)
def ingest_pipeline():
    add_releases_to_lake(
        validate_releases(
            join_releases([
                fetch_kubernetes_releases(),
                fetch_dagster_releases(),
                fetch_knative_eventing_releases(),
                fetch_knative_serving_releases(),
                fetch_cloudfoundry_cf_deployment_releases(),
                fetch_cloudfoundry_cf_for_k8s_releases(),
            ])
        )
    )



from dagster import pipeline, repository, solid, ModeDefinition, PresetDefinition, configured, default_executors, schedule
from dagster.utils import file_relative_path
from dagster_celery_k8s import celery_k8s_job_executor
from dagster_gcp import gcs_resource
from dagster_gcp.gcs import gcs_plus_default_intermediate_storage_defs
from dagster_ge.factory import ge_data_context

from github import fetch_github_releases
from releases import add_releases_to_lake, validate_releases, join_releases
from resources.datalake import fs_datalake_resource, gcs_datalake_resource
from resources.datamart import postgres_datamart_resource, bigquery_datamart_resource

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "datalake": fs_datalake_resource,
        "datamart": postgres_datamart_resource,
        "ge_data_context": ge_data_context,
    },
)

prod_mode = ModeDefinition(
    name="prod",
    intermediate_storage_defs=gcs_plus_default_intermediate_storage_defs,
    resource_defs={
        "datalake": gcs_datalake_resource,
        "datamart": bigquery_datamart_resource,
        "ge_data_context": ge_data_context,
        'gcs': gcs_resource
    },
    executor_defs=default_executors + [celery_k8s_job_executor],
)


@solid
def update_dwh_table(_):
    pass


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
    mode_defs=[local_mode, prod_mode],
    preset_defs=[
        PresetDefinition(
            name="default",
            mode="local",
            run_config={
                "resources": {
                    "datamart": {
                        "config": {
                            "hostname": "localhost",
                            "username": "dagster",
                            "password": "dagster",
                            "db_name": "test"
                        }
                    },
                    "datalake": {
                        "config": {
                            "base_path": file_relative_path(__file__, 'tmp/datalake'),
                        }
                    },
                    "ge_data_context": {
                        "config": {
                            "ge_root_dir": file_relative_path(__file__, "great_expectations")
                        }
                    }
                },
                "execution": {"multiprocess": {"config": {"max_concurrent": 0}}},  # 0 -> Autodetect #CPU cores
                "storage": {"filesystem": {}},
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
            },
        ),
        PresetDefinition(
            name="GCP",
            mode="prod",
            run_config={
                "resources": {
                    "datamart": {
                        "config": {
                            "project": "mrdavidlaing",
                        }
                    },
                    "datalake": {
                        "config": {
                            "bucket": "software_releases_datalake",
                        }
                    },
                    "ge_data_context": {
                        "config": {
                            "ge_root_dir": file_relative_path(__file__, "great_expectations")
                        }
                    }
                },
                "execution": {
                    "celery-k8s": {
                        "config": {
                            "job_namespace": "dagster",
                            "env_config_maps": ["dagster-pipeline-env"],
                            "env_secrets": ["software-releases-dwh-secrets"],
                            "job_image": "eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster:latest",
                            "image_pull_policy": "Always",

                        }
                    }
                },
                "intermediate_storage": {"gcs": {"config": {"gcs_bucket": "software_releases_datalake"}}},
                "loggers": {"console": {"config": {"log_level": "INFO"}}},
            },
        )
    ],
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


@pipeline(
    mode_defs=[local_mode, prod_mode],
)
def populate_dwh_pipeline():
    update_dwh_table()


@schedule(
    cron_schedule="0 6 * * *", pipeline_name="ingest_pipeline", mode="prod"
)  # Every day at 06:00
def daily_execute_ingest_pipeline(_context):
    return {
        "resources": {
            "datamart": {
                "config": {
                    "project": "mrdavidlaing",
                }
            },
            "datalake": {
                "config": {
                    "bucket": "software_releases_datalake",
                }
            },
            "ge_data_context": {
                "config": {
                    "ge_root_dir": file_relative_path(__file__, "great_expectations")
                }
            }
        },
        "execution": {
            "celery-k8s": {
                "config": {
                    "job_namespace": "dagster",
                    "env_config_maps": ["dagster-pipeline-env"],
                    "env_secrets": ["software-releases-dwh-secrets"],
                    "job_image": "eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster:latest",
                    "image_pull_policy": "Always",

                }
            }
        },
        "intermediate_storage": {"gcs": {"config": {"gcs_bucket": "software_releases_datalake"}}},
        "loggers": {"console": {"config": {"log_level": "INFO"}}},
    }


@repository
def software_releases_repository():
    return [ingest_pipeline, populate_dwh_pipeline, daily_execute_ingest_pipeline]

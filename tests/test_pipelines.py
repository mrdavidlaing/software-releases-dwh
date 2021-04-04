from dagster import DagsterInstance, execute_pipeline, reconstructable

from pipelines import ingest_pipeline


def test_ingest_pipeline_e2e():
    result = execute_pipeline(
        pipeline=reconstructable(ingest_pipeline),
        preset="local_inmemory",
        instance=DagsterInstance.get(),
    )

    assert result.success

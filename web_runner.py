import os

from dagster import DagsterInstance, execute_pipeline, reconstructable
from flask import Flask, request, abort, Response, stream_with_context

from pipelines import ingest_pipeline

app = Flask(__name__)


def execute_ingest_pipeline():
    yield 'Starting to execute ingest_pipeline...'
    try:
        dagster_inst = DagsterInstance.get()
        result = execute_pipeline(
            pipeline=reconstructable(ingest_pipeline),
            preset="gcp",
            instance=dagster_inst,
        )
        assert result.success
        yield 'Done!'
    except Exception as e:
        yield 'Failed:'
        yield str(e)


@app.route('/execute_pipeline')
def run_pipelines():
    if request.args.get('pipeline') == "ingest_pipeline":
        return Response(stream_with_context(execute_ingest_pipeline()))
    else:
        abort(403)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 8080))

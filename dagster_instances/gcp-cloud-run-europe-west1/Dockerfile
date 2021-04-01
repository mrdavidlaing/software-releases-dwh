ARG BASE_IMAGE=python:3.7-slim
FROM "${BASE_IMAGE}"

ARG DAGSTER_VERSION=0.11.2

# ==> Dagster base layer
RUN echo "==> Adding Dagster base layer" \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    && pip install --no-cache-dir \
        # Core
        dagster==${DAGSTER_VERSION} \
        dagster-graphql==${DAGSTER_VERSION} \
        dagit==${DAGSTER_VERSION} \
        dagster-postgres==${DAGSTER_VERSION} \
        # Cloud
        dagster-gcp==${DAGSTER_VERSION} \
        # Other
        dagster-ge==${DAGSTER_VERSION} \
    # Cleanup
    && echo "==> Total size BEFORE cleanup: $(du -sh --exclude=/proc /)" \
    && echo "Removing: pycache files" && find / -regex '^.*\(__pycache__\|\.py[co]\)$' -delete \
    && echo "Removing: apt cache" && rm -rf /var/lib/apt/lists/* \
    && echo "==> Total size AFTER cleanup: $(du -sh --exclude=/proc /)"

# Setup $DAGSTER_HOME
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
ADD dagster_instances/gcp-cloud-run-europe-west1/ $DAGSTER_HOME/

# ==> Add user code dependencies - in a separate layer to speed up future builds
COPY tmp/requirements.txt /
RUN  pip install --no-cache-dir -r /requirements.txt \
    # Cleanup
    && echo "==> Total size BEFORE cleanup: $(du -sh --exclude=/proc /)" \
    && echo "Removing: pycache files" &&  find / -regex '^.*\(__pycache__\|\.py[co]\)$' -delete \
    && echo "==> Total size AFTER cleanup: $(du -sh --exclude=/proc /)"

# ==> Add user code
ENV DAGSTER_WORKSPACE=/opt/dagster/workspace/
COPY . $DAGSTER_WORKSPACE
WORKDIR $DAGSTER_WORKSPACE

# ==> Starts a simple flash webserver on 8080 that executes pipelines via /execute_pipeline?pipeline=<pipline name>
EXPOSE 8080
CMD python web_runner.py
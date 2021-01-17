ARG BASE_IMAGE=python:3.7-slim
FROM "${BASE_IMAGE}"

ARG DAGSTER_VERSION=0.10.0

# ==> Add Dagster layer
RUN \
# Dagster
    pip install \
      dagster==${DAGSTER_VERSION} \
      dagster-postgres==${DAGSTER_VERSION} \
      dagster-celery[flower,kubernetes]==${DAGSTER_VERSION} \
      dagster-k8s==${DAGSTER_VERSION} \
      dagster-celery-k8s==${DAGSTER_VERSION} \
# Cleanup
    &&  rm -rf /var/lib/apt/lists/ \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python2.7 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile \
    &&  find / -regex '^.*\(__pycache__\|\.py[co]\)$' -delete

# ==> Add user code dependencies - in a separate layer to speed up future builds
COPY Pipfile* /
RUN     pip install pipenv \
    &&  cd / && pipenv lock --requirements > requirements.txt \
    &&  pip install -r /requirements.txt \
# Cleanup
    &&  rm -rf /root/.cache \
    &&  find / -regex '^.*\(__pycache__\|\.py[co]\)$' -delete

# ==> Add user code
COPY . /dagster_workspace
WORKDIR /dagster_workspace
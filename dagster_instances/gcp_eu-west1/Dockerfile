# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.7-slim

# Install extra dependencies needed by Dagster
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		cron \
	; \
	rm -rf /var/lib/apt/lists/*

# Install dependencies - is a separate layer to speed up future builds
RUN pip install pipenv
COPY Pipfile* /
RUN cd / && pipenv lock --requirements > requirements.txt
RUN pip install -r /requirements.txt

COPY . /
WORKDIR /
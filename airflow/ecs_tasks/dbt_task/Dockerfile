##
#  Generic dockerfile for dbt image building.
#  This file is fetched from dbt-core official repo and what changed according to need source: https://github.com/dbt-labs/dbt-core/blob/v1.2.3/docker/Dockerfile
##

# Top level build args
ARG build_for=linux/amd64


##
# base image (abstract)
##
FROM --platform=$build_for python:3.7.16-slim-bullseye

#ARG dbt_postgres_ref=dbt-core@v1.2.3

## General ARGs
ARG DBT_USER_HOME=/usr/app/dbt/

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
  git \
  ssh-client \
  software-properties-common \
  make \
  curl \
  unzip \
  build-essential \
  ca-certificates \
  libpq-dev \
  && apt-get clean \
  && rm -rf \
  /var/lib/apt/lists/* \
  /tmp/* \
  /var/tmp/* \
  && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
  && unzip awscliv2.zip \
  && ./aws/install

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# Set docker basics

WORKDIR ${DBT_USER_HOME}
VOLUME /usr/app

#FROM base as dbt-postgres
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/dbt-core@v1.2.3#egg=dbt-postgres&subdirectory=plugins/postgres"

COPY script/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
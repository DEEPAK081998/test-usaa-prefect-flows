# BUILD: docker build --rm -t airflow .
# ORIGINAL SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ARG PROJECT_NAME=test_project
# Airflow
ARG AIRFLOW_VERSION=2.2.2
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_GPL_UNIDECODE=yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        libcurl4-openssl-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        ${buildDeps} \
        sudo \
        python3-pip \
        python3-requests \
        default-mysql-client \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

RUN pip install -U pip setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install sqlalchemy \
    && pip install wtforms \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[async,crypto,celery,kubernetes,jdbc,password,postgres,s3,slack,amazon,google_auth]==${AIRFLOW_VERSION} \
    && pip install werkzeug \
    && pip install redis \
    && pip install celery[redis] \
    && pip install celery[sqs] \
    && pip install flask_oauthlib \
    && pip install psycopg2-binary \
    && pip install tornado \
    && pip install boto3 \
    && pip install cryptography \
    && pip install pandas \
    && pip install numpy \
    && pip install toolz \
    && pip install jinja2 \
    && pip install click \
    && pip install docker

RUN apt-get purge --auto-remove -yqq ${buildDeps} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY config/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY config/webserver_config.py ${AIRFLOW_HOME}/webserver_config.py
COPY common ${AIRFLOW_HOME}/common

COPY projects/${PROJECT_NAME}/dags ${AIRFLOW_HOME}/dags
COPY projects/${PROJECT_NAME}/plugins ${AIRFLOW_HOME}/plugins

COPY projects/${PROJECT_NAME}/requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --user -r ${AIRFLOW_HOME}/requirements.txt

ENV PYTHONPATH ${AIRFLOW_HOME}
ENV PYTHONPATH ${AIRFLOW_HOME}/common

EXPOSE 8080 5555 8793

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
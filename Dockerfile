FROM python:3.6-slim

ARG AIRFLOW_VERSION=2.1.0
ARG AIRFLOW_HOME=/usr/local/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes

RUN set -ex \
    && buildDeps=' \
    freetds-dev \
    python3-dev \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
    $buildDeps \
    freetds-bin \
    build-essential \
    python3-pip \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

RUN pip install apache-airflow==${AIRFLOW_VERSION} ipdb dbt wtforms==2.3.3
RUN mkdir -p /root/airflow/dags

ADD . /app
ADD examples/example_basic.py /root/airflow/dags/example_basic.py
ADD examples/example_airflow_dbt.py /root/airflow/dags/example_airflow_dbt.py
ADD examples/example_airbyte_operator.py /root/airflow/dags/example_airbyte_operator.py
ADD profiles.yml /home/root/.dbt/profiles.yml
ADD profiles.yml /root/.dbt/profiles.yml

WORKDIR /app
RUN pip install -e .

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
FROM python:3.8

ARG AIRFLOW_VERSION=2.3.1
ARG AIRFLOW_HOME=/usr/local/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
ENV AIRFLOW_URL = localhost:8080

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
    vim \
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


RUN python -m venv ~/.virtualenvs/datacoves
COPY requirements.txt .
RUN ~/.virtualenvs/datacoves/bin/pip install -U pip
RUN ~/.virtualenvs/datacoves/bin/pip install -r requirements.txt

RUN pip install apache-airflow==${AIRFLOW_VERSION} ipdb wtforms
RUN mkdir -p /root/airflow/dags

ADD dagfactory/ms_teams_webhook_hook.py /root/airflow/plugins/ms_teams_webhook_hook.py
ADD dagfactory/ms_teams_webhook_operator.py /root/airflow/plugins/ms_teams_webhook_operator.py

ADD . /app
ADD examples/example_basic.py /root/airflow/dags/example_basic.py
ADD examples/example_airflow_dbt.py /root/airflow/dags/example_airflow_dbt.py
ADD examples/example_airbyte_operator.py /root/airflow/dags/example_airbyte_operator.py
ADD examples/microsoft_teams.py /root/airflow/callback/microsoft_teams.py



WORKDIR /app
RUN pip install -e .

ENTRYPOINT ["/app/scripts/entrypoint.sh"]

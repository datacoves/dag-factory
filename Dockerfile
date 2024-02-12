FROM python:3.8

ARG AIRFLOW_VERSION=2.8.1
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


ADD . /app
ADD examples/dags /root/airflow/dags
ADD examples/plugins/ /root/airflow/plugins/


WORKDIR /app
RUN pip install -e .

ENTRYPOINT ["/app/scripts/entrypoint.sh"]

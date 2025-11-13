FROM python:3.12-slim

ENV AIRFLOW_HOME=/opt/airflow \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc build-essential libpq-dev curl tini bash ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
RUN pip install --no-cache-dir "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"

RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins /opt/airflow/scripts
WORKDIR ${AIRFLOW_HOME}

ARG AIRFLOW_UID=50000
RUN useradd -ms /bin/bash -u ${AIRFLOW_UID} airflow \
 && chown -R ${AIRFLOW_UID}:0 /opt/airflow \
 && chmod -R g+w /opt/airflow

COPY scripts/entrypoint.sh /opt/airflow/scripts/entrypoint.sh
RUN sed -i 's/\r$//' /opt/airflow/scripts/entrypoint.sh && chmod +x /opt/airflow/scripts/entrypoint.sh

USER ${AIRFLOW_UID}
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/airflow/scripts/entrypoint.sh"]

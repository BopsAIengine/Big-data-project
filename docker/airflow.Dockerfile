FROM apache/airflow:slim-2.10.4-python3.12

# install gcloud and set to PATH
WORKDIR /gcp
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-arm.tar.gz \
    && tar -xf google-cloud-cli-linux-arm.tar.gz \
    && rm google-cloud-cli-linux-arm.tar.gz
RUN ./google-cloud-sdk/install.sh
ENV PATH=$PATH:/gcp/google-cloud-sdk/bin

WORKDIR /opt/airflow

# ETL Dependencies
ENV POETRY_HOME=/opt/pysetup/venv
ENV PATH="${POETRY_HOME}:${PATH}"

COPY pyproject.toml /opt/airflow/
RUN pip install -U pip
RUN pip install psycopg2-binary==2.9.10 apache-airflow-providers-postgres==5.11.0 apache-airflow-providers-google==15.1.0

# (Updated May 2025) ML Code Dependencies - No need to install in airflow image, since it execute through DockerOperator
# COPY code/models/requirements.txt requirements-models.txt
# RUN pip install --no-cache-dir -r requirements-models.txt
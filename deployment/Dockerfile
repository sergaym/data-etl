# Use Python 3.11 Airflow image
FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-client \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for pip operations
USER airflow

# Install pipenv
RUN pip install --no-cache-dir pipenv --user

# Copy environment file first
COPY --chown=airflow:root .env /opt/airflow/

# Copy Pipfile and Pipfile.lock for better caching
COPY --chown=airflow:root Pipfile* /opt/airflow/

# Install dependencies using pipenv
WORKDIR /opt/airflow
RUN pipenv install --system --deploy

# Copy project files
COPY --chown=airflow:root . /opt/airflow/

# Install project package
RUN pip install -e .

# Environment variables
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHON_VERSION=3.11

# Health check
HEALTHCHECK CMD ["airflow", "jobs", "check", "--job-type", "SchedulerJob", "--hostname", "$(hostname)"] 
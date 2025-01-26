# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pipenv
RUN pip install --no-cache-dir pipenv

# Copy Pipfile and Pipfile.lock
COPY Pipfile Pipfile.lock ./

# Install dependencies using pipenv
RUN pipenv install --deploy --system

# Copy project files
COPY src/ ./src/
COPY deployment/dags/ ${AIRFLOW_HOME}/dags/
COPY .env .

# Create directory for data files
RUN mkdir -p /app/data/meter_readings

# Set environment variables
ENV PYTHONPATH=/app
ENV AIRFLOW_HOME=/app/airflow

# Create Airflow directories
RUN mkdir -p ${AIRFLOW_HOME}/logs

# Initialize Airflow database
RUN airflow db init

# Create Airflow user
RUN airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Expose Airflow web server port
EXPOSE 8080

# Start Airflow webserver and scheduler
CMD ["sh", "-c", "airflow webserver -p 8080 & airflow scheduler"] 
# Use the official Airflow image with Python 3.11 as the base
FROM apache/airflow:2.10.4-python3.11

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy the environment file from the deployment folder
COPY .env .

# Copy Pipfile, Pipfile.lock, and setup.py from the project root (build context is the parent folder)
COPY Pipfile Pipfile.lock setup.py ./

# Copy the source code package from the "src" directory
COPY src ./src

# Switch to root to change ownership so the Airflow user can write to the src directory.
USER root
# Use the environment variable AIRFLOW_UID if defined, otherwise default to 50000.
RUN chown -R ${AIRFLOW_UID:-50000}:${AIRFLOW_UID:-50000} /opt/airflow/src
USER airflow

# Install pipenv and use it to install dependencies.
RUN pip install pipenv && pipenv install --deploy --ignore-pipfile

# Install your package in editable mode so that your DAGs can import it.
RUN pip install -e .

# Copy DAGs and plugins from the deployment folder into the container.
COPY deployment/dags /opt/airflow/dags
COPY deployment/plugins /opt/airflow/plugins

# (Optional) If you want tests inside the image:
# COPY deployment/tests /opt/airflow/tests

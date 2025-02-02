# Airflow Docker Setup

This repository provides a simple Apache Airflow environment using Docker Compose along with a sample DAG (`simple_dag.py`) that prints "Hello, Airflow!".

## Repository Structure

```
airflow-docker/
├── dags/                  # Contains DAGs (including simple_dag.py)
├── db/                    # Contains the SQLite database (airflow.db)
├── logs/                  # Airflow log files
├── plugins/               # Airflow plugins (if any)
├── docker-compose.yaml    # Docker Compose configuration
└── readme.md              # This file
```

## Quick Start

1. **Prepare the Database**
   Ensure the `db` directory exists and create a writable SQLite file:
   ```bash
   mkdir -p db
   touch db/airflow.db
   chmod 666 db/airflow.db
   ```

2. **Initialize the Airflow Database**
   Run the following command to initialize the metadata database:
   ```bash
   docker-compose run --rm airflow-webserver airflow db init
   ```

3. **Start Airflow**
   Launch the containers:
   ```bash
   docker-compose up -d
   ```

4. **Access the Airflow UI**
   Open your browser at [http://localhost:8080](http://localhost:8080).  
   Note: New DAGs (like `simple_hello_dag`) are created in a paused state. Toggle your DAG to “On” in the UI and trigger it manually if desired.

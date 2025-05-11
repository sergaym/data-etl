# **📊 Smart Meter Readings ETL Pipeline**  

## **📌 Overview**  
This repository contains a **technical Data Engineering case study**.  

### **✅ Project Scope**  
- ✅ **Extracts, transforms, and loads** smart meter readings from **JSON & SQLite**.  
- ✅ **Stores raw data for traceability** and **transforms data for reporting**.  
- ✅ **Implements a modular, scalable architecture**.  
- ✅ **Lays the foundation for automation via Airflow & cloud migration**.  

This version **focuses on batch processing**, with **orchestration, monitoring, and real-time ingestion planned**.

---

## **⚙️ ETL Architecture & Data Flow**  
This ETL pipeline follows a **modular & scalable architecture**, ensuring **maintainability, automation, and easy extensibility**.

```
src/
├── extraction/         # Extracts data from JSON files and SQLite
├── transformation/     # Processes and transforms the data
├── loading/            # Loads raw & transformed data into PostgreSQL
├── pipelines/          # Main ETL orchestration
└── utils/              # Shared utilities (logging, config)
```

| **Step**            | **Description** |
|---------------------|----------------|
| **Extract**         | Reads raw data from JSON & SQLite. |
| **Store Raw Data**  | Saves extracted data in PostgreSQL (`raw_data` schema). |
| **Transform**       | Processes raw data into structured analytics. |
| **Load Analytics**  | Saves processed data into PostgreSQL (`analytics` schema). |

---

## **📌 Handling `reference_date` in the Pipeline**  

### **🔹 Current Implementation**  
- **`reference_date` is set to `2021-01-01` by default**, as per the case study requirements.  
- **Raw data is always ingested fully** → we store all available readings and metadata.  
- **Analytics tables reference `reference_date`** to filter and aggregate data.  

### **🔹 Future Improvements for `reference_date` Handling**  
✔ **Update how we fetch raw data** → Instead of reading everything, **fetch data incrementally** (e.g., from dated folders in S3 or an API with timestamp-based filtering).  
✔ **Support dynamic `reference_date` changes** → Right now, **it's hardcoded for January 1, 2021**, but in real scenarios, we must **handle different reporting periods** dynamically.  
✔ **Implement a configuration layer** → Define how `reference_date` should be set for different reports (fixed vs. rolling periods).  
✔ **Improve Airflow DAG parameterization** → Allow running different `reference_date` values in Airflow dynamically.

---

## **📌 Data Storage Strategy**
This ETL pipeline **preserves both raw and transformed data** to support both **analytics and direct querying** by analysts.

| **Schema**    | **Table**                    | **Purpose** |
|--------------|-----------------------------|-------------|
| **raw_data** | `raw_meter_readings`        | Stores raw JSON readings for full traceability. |
| **raw_data** | `raw_agreements`            | Stores raw contract agreement records. |
| **raw_data** | `raw_products`              | Stores product/tariff details. |
| **raw_data** | `raw_meterpoints`           | Stores raw meterpoint metadata. |
| **analytics** | `active_agreements`         | Agreements valid on **reference_date**. |
| **analytics** | `half_hourly_consumption`   | Aggregated consumption per half-hour. |
| **analytics** | `daily_product_consumption` | Aggregated daily consumption per product. |

✅ **Why this approach?**  
- **Raw data is stored before transformation** to ensure **data traceability & recovery**.  
- **Transformed data is optimized for analytics & reporting**.

---

## **📌 Known Limitations & Technical Trade-offs**
| **Category** | **Current Limitation** | **Planned Improvements** |
|-------------|-----------------|-------------------------|
| **Scope** | Focuses on **batch ETL**; **event-driven processing not yet implemented**. | Implement **real-time data ingestion** for continuous updates. |
| **Performance** | Currently **single-threaded**, in-memory transformations. | **Migrate to PySpark** for large-scale distributed processing. |
| **Orchestration** | No automated scheduling yet. | Integrate **Airflow/Dagster** for automation & monitoring. |
| **Error Handling** | Basic logging & schema validation. | **Enhance failure recovery** with retries & alerting. |
| **Reference Date Handling** | Hardcoded to `2021-01-01` in analytics. | **Allow configurable reference periods & dynamic execution.** |
| **Cloud Storage** | Reads raw data from local files. | **Migrate raw storage to S3 / cloud-based DB.** |
---

## **📌 Future Roadmap**
| **Feature** | **Why?** | **Priority** |
|------------|---------|-------------|
| **S3 Integration & Cloud Migration** | Move raw data to **S3**, replace SQLite with **PostgreSQL RDS**. | 🚨 Critical |
| **Reference Date Handling Improvements** | Implement **dynamic date selection & automatic period filtering**. | 🚀 High |
| **Airflow DAG Optimization** | Implement modular **task-based orchestration**. | 🔥 High |
| **Monitoring (Grafana/Prometheus)** | Ensure observability & alerts. | 🔍 Medium |
| **CI/CD (GitHub Actions)** | Automate testing & deployments. | ✅ Medium |
| **Test Coverage & Quality** | Ensure reliability & maintainability. | ⚡ Medium |
| **Schema-Based Data Validation** | Define clear **Pydantic schemas** for structured validation before ingestion. | ✅ Medium |
| **Analytics Modeling (dbt)** | Introduce **dbt** for transformation layer consistency & reusability. | ✅ Medium |
---

## **📌 Usage Instructions**
### **1️⃣ Install Dependencies**
```bash
pip install -e .
```
or using Pipenv:
```bash
pipenv install
```

### **2️⃣ Run the ETL Manually**
To process data for a specific date:
```bash
python -m src.pipelines.etl.py --reference_date 2021-01-01
```

### **3️⃣ Run ETL with Airflow** (🚧 Draft Implementation)
We have drafted an Airflow-based orchestration setup (not yet production-ready):

```bash
# Note: This is a draft implementation
airflow dags trigger meter_readings_etl
```
🔹 **Airflow Status:**
- ✅ DAG logic implemented (`meter_readings_etl.py`).
- ✅ Configured for **batch processing** (real-time triggers planned).
- ✅ Integrated with **Docker for local testing**.
- 🔜 **Pending:** Production deployment validation & security enhancements.

---

## **📌 Deployment: Airflow + Docker**
### **🔹 Deployment Stack**
| **Component**         | **Technology**        | **Purpose** |
|----------------------|---------------------|-------------|
| **Orchestration**    | **Apache Airflow**  | Manages ETL scheduling. |
| **Containerization** | **Docker**          | Ensures consistent deployment. |
| **Database**         | **PostgreSQL**      | Stores raw & transformed data. |

### **🔹 Running Airflow Locally**
```bash
cd deployment
docker-compose up -d
```
To access the Airflow UI, open **`http://localhost:8080`**.

---

## **📌 Bonus: Streamlit App for Analytics** 🎉  
A **Streamlit-based web app** is included for interactive data exploration.

### **✅ Features**
- View **raw & transformed tables** dynamically.  
- Run **custom SQL queries** on PostgreSQL.  
- Visualize **aggregated consumption trends & agreement activations**.  

### **📌 How to Run**
```bash
streamlit run playground/streamlit-app/main.py
```
Then, open the app in your browser at **`http://localhost:8501`**.

---

## **📩 Contact & Support**
For any issues or feature requests, please **open a GitHub Issue**.  
For direct queries, contact 📧 **sergioayala.contacto@gmail.com**.

---

### **📌 TL;DR (Summary)**
✅ **What it does:** Extracts, transforms & loads smart meter data into PostgreSQL.  
✅ **How to run:** `python -m src.pipelines.etl --reference_date 2021-01-01`.  
✅ **Next steps:** Improve **reference date handling, automate with Airflow**, migrate to **S3/cloud**, and enhance **monitoring & testing**.  

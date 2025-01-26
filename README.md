# **📊 Smart Meter Readings ETL Pipeline**  

## **📌 Overview**  
This repository contains **Sergio Ayala's solution** for the **technical Data Engineering case study** as part of his application for the **Head of Analytics Platform Engineering** role.  

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

## **📌 Data Storage Strategy**
This ETL pipeline **preserves both raw and transformed data** to support both **analytics and direct querying** by analysts.

| **Schema**    | **Table**                    | **Purpose** |
|--------------|-----------------------------|-------------|
| **raw_data** | `raw_meter_readings`        | Stores raw JSON readings for full traceability. |
| **raw_data** | `raw_agreements`            | Stores raw contract agreement records. |
| **raw_data** | `raw_products`              | Stores product/tariff details. |
| **raw_data** | `raw_meterpoints`           | Stores raw meterpoint metadata. |
| **analytics** | `active_agreements`         | Agreements valid on 2021-01-01 for reporting. |
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
| **Cloud Storage** | Reads raw data from local files. | **Migrate raw storage to S3 / cloud-based DB.** |

---

## **📌 Future Roadmap**
| **Feature** | **Why?** | **Priority** |
|------------|---------|-------------|
| **S3 Integration & Cloud Migration** | Move raw data to **S3**, replace SQLite with **PostgreSQL RDS**. | 🚨 Critical |
| **Airflow DAG Optimization** | Implement modular **task-based orchestration**. | 🚀 High |
| **Test Coverage & Quality** | Ensure reliability & maintainability. | 🔥 High |
| **Event-Based Processing** | Support near real-time data ingestion. | ⚡ Medium |
| **Monitoring (Grafana/Prometheus)** | Ensure observability & alerts. | 🔍 Medium |
| **CI/CD (GitHub Actions)** | Automate testing & deployments. | ✅ Medium |

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
python -m src.pipelines.etl --reference_date 2023-01-01
```

### **3️⃣ Run ETL with Airflow** (🚧 Draft Implementation)
We have drafted an Airflow-based orchestration setup (not yet production-ready):

```bash
# Note: This is a draft implementation
airflow dags trigger meter_readings_etl
```

🔹 **Current Status:**
- ✅ DAG logic implemented (`meter_readings_etl.py`)
- ✅ Docker configuration drafted
- ❌ Not yet tested in production
- ❌ Pending proper deployment setup

🔹 **Next Steps for Airflow Implementation:**
1. Test Docker setup
2. Add monitoring & alerting
3. Configure proper security
4. Set up CI/CD pipeline
5. Document deployment process

For now, please use the manual ETL execution method described above.

---

## **📌 Bonus: Streamlit App for Analytics Table Exploration** 🎉  

To make data exploration **easier for analysts**, this repository includes a **Streamlit-based web application** that allows users to **interactively explore both raw and transformed analytics tables** stored in PostgreSQL.  

### **✅ Features**
- View **raw & transformed tables** dynamically.  
- Run **custom SQL queries** on the database.  
- Visualize **aggregated consumption trends & agreement activations**.  

### **📌 How to Run the Streamlit App**
```bash
streamlit run playground/streamlit-app/main.py
```
Then, open the app in your browser at **`http://localhost:8501`**.

---

## **📌 ETL Pipeline Architecture**

### **🔹 Two-Task Pipeline Design**
The ETL pipeline is divided into two distinct tasks for better scalability and data integrity:

1. **Task 1: Extract & Store Raw**
   - Extracts data from JSON files and SQLite
   - Stores in PostgreSQL `raw_data` schema
   - Ensures data traceability

2. **Task 2: Transform & Load Analytics**
   - Reads from PostgreSQL raw tables
   - Transforms data for analytics
   - Loads to `analytics` schema

### **🔹 Why This Approach?**
We chose to store raw data first, then transform because:
- ✅ Ensures data integrity and traceability
- ✅ Enables independent access to raw data
- ✅ Supports better error recovery
- ✅ Facilitates debugging and reprocessing

### **🔹 Airflow Implementation**
- DAG configured to run tasks sequentially
- Task 1 runs at 8:30 AM (after source updates)
- Task 2 runs after Task 1 completes
- Docker setup drafted (not production-tested)

---

## **📩 Contact & Support**
For any issues or feature requests, please **open a GitHub Issue**.  
For direct queries, contact 📧 **sergioayala.contacto@gmail.com**.

---

### **📌 TL;DR (Summary)**
✅ **What it does:** Extracts, transforms & loads smart meter data into PostgreSQL.  
✅ **How to run:** `python -m src.pipelines.etl --reference_date YYYY-MM-DD`.  
✅ **Next steps:** Automate with **Airflow**, migrate to **S3/cloud**, and improve **monitoring & testing**.  


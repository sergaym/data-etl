Here's the **final refined README**, including the **Streamlit App as a bonus feature** at the bottom while keeping the professional clarity expected for the **Head of Analytics Platform Engineering role**. 🚀  

---

# **📊 Smart Meter Readings ETL Pipeline**  

## **📌 Overview**  
This repository contains **Sergio Ayala’s solution** for the **technical Data Engineering case study** as part of his application for the **Head of Analytics Platform Engineering** role.  

### **✅ Project Scope**  
The challenge requires designing and implementing an **ETL pipeline** that:  
- ✅ **Processes smart meter readings** from **JSON & SQLite data sources**.  
- ✅ **Stores raw data** for analysts & **transforms data for reporting**.  
- ✅ **Implements a modular architecture** to ensure **scalability, automation, and maintainability**.  
- ✅ **Lays the foundation for future automation** via **Airflow/Dagster**.  

This solution provides a **strong foundation**, with **batch processing & orchestration planned for future iterations**.

---

## **⚙️ Design Choices & Architecture**  
This ETL pipeline follows a **modular & scalable architecture**, ensuring **maintainability, team collaboration, and easy extensibility**.

### **🔹 Architecture Overview**
```
src/
├── extraction/         # Extracts data from JSON files and SQLite
├── transformation/     # Processes and transforms the data
├── loading/            # Loads raw & transformed data into PostgreSQL
├── pipelines/          # Main ETL orchestration
└── utils/              # Shared utilities (logging, config)
```

### **🔹 Key Design Principles**
| **Principle**             | **Implementation** |
|---------------------------|-------------------|
| **🔹 Modular Structure** | Each ETL step is **self-contained**, allowing **parallel development** across teams. |
| **🔹 Data Storage Strategy** | Stores **raw data** (`raw_data` schema) & **transformed data** (`analytics` schema). |
| **🔹 Data Quality & Monitoring** | Enforces **schema validation**, **type checking**, & **structured logging**. |
| **🔹 Scalability & Extensibility** | Designed for **batch & future real-time processing** (Airflow, PySpark). |

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
- **Raw data is preserved for validation, auditing & deep analysis.**  
- **Transformed data is optimized for BI & dashboards.**  

---

## **📌 Known Limitations & Technical Trade-offs**
| **Category** | **Current Limitation** | **Planned Improvements** |
|-------------|-----------------|-------------------------|
| **Scope** | Focuses on **batch ETL**; **event-driven processing not yet implemented**. | Implement **real-time data ingestion** for continuous updates. |
| **Performance** | Currently **single-threaded**, in-memory transformations. | **Migrate to PySpark** for large-scale distributed processing. |
| **Orchestration** | No automated scheduling yet. | Integrate **Airflow/Dagster** for automation & monitoring. |
| **Error Handling** | Basic logging & schema validation. | **Enhance failure recovery** with retries & alerting. |

---

## **📌 Future Roadmap**
| **Feature** | **Why?** | **Priority** |
|------------|---------|-------------|
| **Airflow/Dagster Orchestration** | Automate scheduling & monitoring. | 🚀 High |
| **Cloud Deployment (AWS/GCP)** | Make ETL cloud-native & scalable. | 🔥 High |
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

### **3️⃣ Run ETL with Airflow**
To schedule the ETL pipeline using **Apache Airflow**:
```bash
airflow dags trigger etl_pipeline
```
🔹 **Make sure Airflow is running** (`airflow webserver` and `airflow scheduler`).  


---

## **🛠️ How to Test**
This repository follows **Test-Driven Development (TDD)** with **unit tests for each ETL step**.

### **1️⃣ Run All Tests**
```bash
pytest tests/
```
or with test coverage:
```bash
pytest --cov=src
```

### **2️⃣ Test Individual Components**
```bash
pytest tests/test_extraction.py
pytest tests/test_transformation.py
pytest tests/test_loading.py
```

✅ **What’s covered?**  
- **Data extraction works correctly** (valid JSON/DB queries).  
- **Transformations return correct outputs** (schema, aggregations, business logic).  
- **Data loads successfully into PostgreSQL** (valid inserts, no duplicates).  

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

## **📩 Contact & Support**
For any issues or feature requests, please **open a GitHub Issue**.  
For direct queries, contact 📧 **sergioayala.contacto@gmail.com**.

---

## **📌 TL;DR (Summary)**
✅ **What it does:** Extracts, transforms & loads smart meter data into PostgreSQL.  
✅ **How to run:** `python -m src.pipelines.etl --reference_date YYYY-MM-DD`.  
✅ **Next steps:** Automate with **Airflow/Dagster**, deploy to **cloud**, and improve **monitoring**. Include tests.

# 🧪 Next Steps: Test Coverage Plan  

## 📌 Overview  
This ETL pipeline requires **comprehensive test coverage** to ensure reliability and maintainability. Below is the test plan and what needs to be implemented next.  

---

## ✅ Planned Test Coverage  

| **Component**       | **Test Type**                | **Description** |
|---------------------|----------------------------|----------------|
| **Extraction**      | Unit & Integration Tests    | Validate correct reading from JSON & database. |
| **Transformation**  | Unit Tests                  | Ensure correct filtering, aggregations, and business rules. |
| **Loading**        | Integration Tests           | Confirm data is written correctly to PostgreSQL. |
| **Airflow DAGs**    | DAG Integrity Checks        | Ensure DAGs are properly defined & tasks execute in order. |
| **End-to-End**      | Full Pipeline Tests         | Simulate a daily run and validate outputs. |

---

## 🔹 Next Steps  
1. **Set up `pytest` as the testing framework**.  
2. **Implement unit tests** for `extraction`, `transformation`, and `loading`.  
3. **Create integration tests** for database interactions.  
4. **Validate Airflow DAG execution using `airflow dags test`**.  
5. **Implement a CI/CD pipeline to automate testing (GitHub Actions).**  

---

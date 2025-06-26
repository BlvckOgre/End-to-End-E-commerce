
# 🛒 Brazilian E-Commerce Data Engineering Pipeline

This project showcases a full end-to-end **data engineering pipeline** built using the **Brazilian Olist e-commerce dataset**, deployed using modern data tools and cloud services. It is structured around the **Medallion Architecture** to simulate production-grade pipelines and transformations.

---

## 📊 Project Overview

This project aims to ingest, clean, transform, and model data from multiple sources into an analytics-ready format. It mirrors real-world complexity by integrating structured and semi-structured data from cloud storage, relational databases, and NoSQL stores.

---

## 🗂 Dataset Summary

The dataset used is a popular e-commerce dataset from Olist, consisting of multiple CSV files representing:

- Customers
- Orders
- Products
- Payments
- Sellers
- Reviews
- Product Category Translations (MongoDB)
- Order Payments (MySQL)

---

## 🧰 Tech Stack

| Area              | Tool / Technology           |
|-------------------|-----------------------------|
| Languages         | PySpark, SQL                |
| Orchestration     | Azure Data Factory          |
| Storage           | Azure Data Lake Storage Gen2 |
| Processing        | Azure Databricks, Google Colab |
| Data Sources      | CSV (GitHub), MySQL, MongoDB |
| Data Warehouse    | Azure Synapse Analytics     |

---

## 🧱 Architecture: Medallion Pattern

```
External Sources (GitHub CSVs, MySQL, MongoDB)
      |
[Azure Data Factory]
      |
ADLS Gen2 - Bronze (Raw Files)
      |
[Azure Databricks: PySpark ETL]
      |
ADLS Gen2 - Silver (Cleaned, Joined)
      |
[Azure Synapse: SQL CTAS, Views]
      |
ADLS Gen2 - Gold (Analytics-Ready Data)
```

---

## ⚙️ Pipeline Stages

### 🔹 1. Ingestion (Bronze Layer)

- **GitHub CSV Files**:
  - Ingested via Data Factory using a parameterized pipeline.
  - Uses a JSON-based config and a Lookup + ForEach + Copy pattern.

- **MySQL (Order Payments)**:
  - Loaded using a custom Data Factory pipeline.
  - MySQL database hosted and accessed securely.

- **MongoDB (Product Category Names)**:
  - Loaded using a Colab notebook and `pymongo`.
  - Accessed directly from Azure Databricks during ETL.

---

### 🔸 2. Transformation (Silver Layer)

- All source files are read in Azure Databricks using PySpark.
- Transformations include:
  - Schema harmonization
  - Date/time format standardization
  - Removal of nulls and duplicates
  - Merging/joining tables with `orders` as the central fact
  - MongoDB product categories are cleaned and matched to product data

- Final intermediate dataset (`df_final`) is saved in Parquet format to the Silver layer.

---

### 🟡 3. Modeling & Serving (Gold Layer)

- Silver data is ingested into **Azure Synapse SQL Pools**.
- SQL-based transformation logic is applied using:
  - **CTAS (Create Table As Select)**
  - **Views** for business logic and reporting KPIs

- Final outputs are written back to ADLS Gen2 (Gold directory) in Parquet format.

---

## 📁 Project Directory Structure

```
/olist-ecommerce-pipeline/
├── notebooks/
│   ├── ingestion_github.ipynb
│   ├── ingestion_mysql_colab.ipynb
│   ├── ingestion_mongodb_colab.ipynb
│   ├── databricks_etl.ipynb
│   └── synapse_final_transforms.sql
├── pipeline_configs/
│   └── github_file_list.json
├── resources/
│   └── architecture_diagram.png (optional)
└── README.md
```

---

## 📈 Sample Business KPIs

These can be generated via SQL or connected to a dashboarding tool like Power BI:

- 🛍 Total orders per state
- ⏱ Average delivery time per seller
- 💳 Distribution of payment methods
- 🧾 Revenue per product category
- ⭐ Average review score over time

---

## 🔒 Security

- All services deployed under the same Azure Resource Group.
- Databricks-to-ADLS connection uses Azure App Registration (Service Principal OAuth).
- Role-Based Access Control (RBAC) used for secure access between services.

---

## 🚀 Future Improvements

- Add Data Quality checks (e.g. with Great Expectations)
- CI/CD setup using GitHub Actions or Azure DevOps
- Implement data partitioning for performance
- Add streaming ingestion (Kafka/Event Hub)

---

## 🧾 Notebooks

All work is encapsulated in notebooks located under the `/notebooks/` folder and can be run in Google Colab, Databricks, or Azure Synapse where applicable.

---

> **Author**: Data Engineering Team  
> **Note**: External data links used for ingestion simulation only. Not committed to this repository.

---

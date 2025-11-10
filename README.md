# 🛠️ Medallion Data Pipeline with Airflow, AWS S3, dbt, and Yandex PostgreSQL

Welcome to my end to end Data Engineering project.  
This repository demonstrates a complete modern data pipeline that integrates cloud storage, workflow orchestration, data modeling, and warehouse automation.  
The project is designed as a real production style portfolio piece that highlights my technical experience with scalable data systems.

---

## 🧱 Data Architecture

This project implements the Medallion Architecture, a layered data design that improves clarity, reliability, and transformation flow.

| Layer | Description |
|-------|-------------|
| 🥉 Bronze Layer | Stores raw JSON data extracted directly from AWS S3 without modifications. |
| 🥈 Silver Layer | Contains cleaned, validated, standardized tables using dbt transformations. |
| 🥇 Gold Layer | Provides business-ready analytical views built using dimensional modeling. |

This architecture ensures transparency, traceability, and separation of responsibility across the transformation journey.

---

## 📖 Project Overview

The goal of this project is to build a fully automated data pipeline that simulates a real analytics environment.

The system includes:
- Automated ingestion of semi-structured data from AWS S3  
- Normalization and cleaning of inconsistent raw inputs  
- Transformation pipeline powered by dbt  
- Curated analytics layer for BI consumption  
- A warehouse hosted on Yandex Cloud PostgreSQL  
- A fully orchestrated workflow via Apache Airflow  

The datasets used in this project represent a fictional e commerce workflow, including customers, orders, products, payments, inventory, shipments, reviews, and warehouses. Each dataset contains intentionally unclean raw fields to simulate realistic source behavior.

---

## 🎯 Project Goals

The main objective is to create a production style data ecosystem capable of supporting reliable analytics.

Key focus areas include:
- Building a scalable ingestion framework  
- Designing SQL models for business reporting  
- Ensuring data quality and consistency  
- Automating each step of the ETL pipeline  
- Implementing Medallion Architecture correctly  
- Demonstrating clean warehouse design and best practices  

This project showcases not only technical skills but also an understanding of modern data engineering patterns.

---

## 🛠️ Technologies Used

| Category | Tools |
|----------|-------|
| Orchestration | Apache Airflow |
| Data Storage | AWS S3 |
| Data Warehouse | Yandex Cloud PostgreSQL |
| Transformations | dbt |
| Programming | Python, SQL |
| Infrastructure | Docker Compose |
| Design | Medallion Architecture |

These tools simulate a real world cloud based analytics environment.

---

## 🚀 Data Pipeline Flow

### 1. **Ingestion from AWS S3 into Bronze Layer**
Raw JSON files stored in S3 are loaded into PostgreSQL exactly as received.  
This includes missing values, inconsistent formatting, corrupted IDs, and irregular strings — just like real operational data sources.

### 2. **Cleaning and Standardization in Silver Layer**
dbt models apply:
- Deduplication  
- String normalization and trimming  
- Removal of invalid characters  
- Enforcement of consistent data types  
- Date validation  
- Normalization of IDs  
- Handling of unknown or invalid values  

Each raw table receives its own corresponding cleaned model.

### 3. **Business Modeling in Gold Layer**
The gold layer is built as a set of **analytics ready views** based on dimensional modeling.  
It includes:
- Product dimension  
- Customer dimension  
- Sales fact view  
- Product review enrichment  
- Inventory and warehouse attributes  

These views can be connected to BI tools for dashboards.

---

## 📊 Data Marts (Gold Layer)

Gold Layer follows a star schema approach. The marts created include:

### • Product Dimension  
Contains product attributes, brand, category, pricing, warehouse location, stock levels, and aggregated review details.

### • Customer Dimension  
Provides standardized customer profiles, geographic attributes, and segments.

### • Sales Fact View  
Combines order items, payments, shipment information, and product details to support revenue and performance analytics.

This structure is designed for fast and intuitive reporting.

---

## 🧪 Data Quality and Reliability

Throughout the pipeline, several quality principles are applied:

- Validation of IDs  
- Controlled fallback values  
- Strict data type enforcement  
- Prevention of broken relationships between tables  
- Removal of invalid characters from raw inputs  
- Date range normalization  
- Referential checks through dbt `ref()` dependencies  

The result is a warehouse that is both clean and trustworthy.

---

## ▶️ How to Run the Project

1. Clone the repository.  
2. Start the Airflow and dbt environment with Docker.  
3. Configure AWS and PostgreSQL connections inside Airflow.  
4. Trigger the `extract_transform` DAG.  
5. Observe the pipeline loading Bronze, Silver, and Gold Layers.  
6. Query the Gold Layer for analytics.

This makes the project easy to run and evaluate.

---

## ☕ About the Author

My name is **Aleksey**, and I’m deeply interested in cloud data platforms, modern warehouse design, and automated pipelines.  
This project was created both for learning and as a demonstration of practical data engineering skills relevant to production environments.

If you find this project useful or interesting, feel free to star the repository or reach out.

---

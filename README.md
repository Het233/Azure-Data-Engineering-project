# Azure Data Lakehouse Platform: End-to-End Retail Sales Analytics

A  **Data Lakehouse Architecture** built on the Microsoft Azure ecosystem. This platform automates the incremental ingestion of corporate retail transaction data, processes it through a unified medallion framework using PySpark, stages a relational star schema.

---

## 🏗️ Architecture & Data Flow

The platform separates responsibilities into four cleanly decoupled layers, allowing processing compute to scale independently of the data warehouse serving layer.

1. **Ingestion (Azure Data Factory):** Utilizes a two-way watermarking look-up pattern to capture incremental source modifications dynamically.
2. **Storage Layer (ADLS Gen2):** Organized cleanly into Medallion zones (**Bronze** for raw ingestion, **Silver** for cleansed/deduplicated records, and **Gold** for dimensional star-schema modeling).
3. **Compute & Transformation Engine (Azure Databricks + PySpark):** Parallelized notebook processing that manages state logic, schema enforcement, and historical **SCD Type 2** data mapping.
4. **Relational Serving Layer (Azure Synapse Serverless SQL):** A zero-copy virtual data warehouse that maps T-SQL views directly over open-source Delta Lake paths, eliminating data redundancy.


---

## 🛠️ Orchestration Pipeline Design

### 1. Ingestion & Watermarking Flow
The ingestion pipeline enforces strict data integrity by tracking states before loading the destination lake.
* **Incremental Delta Capture:** Two parallel `Lookup` activities fetch the last processed watermark timestamp and the current maximum source timestamp.
* **State Management:** The `Stored Procedure` updates the control table *only* after the `Copy Data` activity safely commits the raw batch to the Bronze container.

### 2. Parallelized Databricks Execution Loop
Once ingestion completes, an `Execute Pipeline` task calls the Databricks notebook compute loop.
* **Resource Optimization:** The pipeline executes the `Gold_dimCustomer`, `Gold_dimProduct`, and `Gold_dimSalesRegion` processing scripts **in parallel**, dramatically slashing cluster runtime.
* **Fact Table Integrity:** The `Gold_factSales` stage is blocked until all lookup dimensions successfully write their surrogate keys (`SK`s).

## 💻 Tech Stack & Tooling
* **Orchestration:** Azure Data Factory (ADF)
* **Compute / Processing:** Azure Databricks (PySpark / Spark SQL)
* **Storage Architecture:** Azure Data Lake Storage Gen2 (Delta Lake / Parquet formats)
* **Data Warehousing / Serving:** Azure Synapse Analytics (Serverless SQL Pools)
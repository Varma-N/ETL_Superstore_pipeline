# 🛠️ Idempotent ETL Pipeline – Superstore Sales Data

> **Production-Ready Incremental ETL with Python, Pandas & MySQL — Featuring Idempotency, Logging, and Composite Key Deduplication**

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-important?logo=mysql)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-green?logo=pandas)
![License](https://img.shields.io/badge/License-MIT-orange)

## 📌 Overview

This project implements a robust, idempotent, and incrementally loading ETL pipeline using Python and MySQL. It processes the Superstore Sales Dataset, demonstrating:

- ✅ Extract: Load raw data from CSV.
- ✅ Transform: Clean column names, standardize date formats.
- ✅ Load: Upsert into MySQL using composite primary key `(order_id, product_id)`.
- ✅ Idempotent Execution: Safe to re-run — no duplicate records.
- ✅ Incremental Loads: Only new `(order_id, product_id)` combinations are inserted.
- ✅ Logging & Metrics: Tracks rows inserted vs. skipped with timestamped logs.

Ideal for learning or extending into production with Airflow, Prefect, cron, or API sources.

## 🗂️ Project Structure
```
ETL_PIPELINE/
│
├── data/
│ └── [Download Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
│
├── logs/
│ └── etl_superstore.log
│
├── src/
│ └── etl_superstore.py
├── requirements.txt
└── README.md
```

## ⚙️ Tech Stack

| Component       | Technology                 |
|----------------|----------------------------|
| Language       | Python 3.8+                |
| Libraries      | pandas, mysql-connector-python, logging |
| Database       | MySQL 8.0+                 |
| IDE            | VS Code / Jupyter (optional) |
| Logging        | File-based with metrics    |

## 🚀 How It Works

1. **Extract**  
   Reads `csv file` into a pandas DataFrame.

2. **Transform**  
   - Cleans column names to snake_case  
   - Converts `Order Date` and `Ship Date` to `YYYY-MM-DD`

3. **Load**  
   - Connects to MySQL table `superstore_orders_table`  
   - Checks for existing `(order_id, product_id)` before INSERT  
   - Logs inserted vs skipped rows

4. **Idempotent & Incremental**  
   - Re-running causes no duplicates  
   - New rows in CSV auto-load on next run

## 🏗️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Varma-N/ETL_Superstore_pipeline
cd ETL_PIPELINE
```
### 2️⃣ Install Dependencies
```bash
pip install -r requirements.txt
```
### 3️⃣ Setup MySQL Database
```sql
CREATE DATABASE etl_superstore;
USE etl_superstore;

CREATE TABLE superstore_orders_table (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    order_date DATE,
    ship_date DATE,
    customer_name VARCHAR(255),
    category VARCHAR(100),
    segment VARCHAR(100),
    sales DECIMAL(10,2),
    quantity INT,
    profit DECIMAL(10,2),
     UNIQUE KEY unique_order_product (order_id, product_id)  -- Composite uniqueness
);
```
### 4️⃣ Run the ETL Pipeline
```bash
python src\etl_superstore.py
```

### 📊 Example Output
```yaml
🚀 Starting ETL Process...
✅ Data extracted from CSV.
✅ Data transformed: columns cleaned and dates formatted.
✅ Load completed! Inserted: 150 | Skipped (duplicates): 10
🎉 ETL completed successfully!
```
### 🌟 Key Learnings
- Building idempotent ETL pipelines.
- Implementing incremental loads with composite keys.
- Using Python for real-world data engineering tasks.
- Preparing for automation & orchestration (cron, Airflow, Prefect).

### 📌 Next Steps
- Automate with cron/Task Scheduler.
- Extend to API extraction (JSON → Transform → Load).
- Orchestrate with Airflow / Prefect.
- Connect MySQL to Power BI / Tableau for reporting.

### 🤝 Contributing
Contributions are welcome! Fork this repo, improve the pipeline, and submit a PR 🚀

### 📜 License
MIT License © 2025


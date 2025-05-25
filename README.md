# E-Commerce Sales Data ETL Pipeline

## Overview

This project implements a scalable and reliable ETL pipeline using **Databricks** and **PySpark** for processing e-commerce sales data. It is designed to clean, transform, and analyze raw data to provide actionable insights for business stakeholders.

---

## Problem Statement

The task is to process raw e-commerce datasets, enrich them with relevant information, and generate aggregate insights. The goal is to support business decisions with curated, high-quality data outputs.

### Source

📁 [Download the datasets](https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing)

---

## Tasks & Deliverables

### ✅ Raw Tables : Bronze Layer

- Load and store the raw data from each source dataset as-is.

### ✅ Cleaned and Standardized Tables : Silver Layer

- Apply transformations to remove invalid and duplicate records.
- Trim whitespace, fix casing, validate data types, and handle nulls.

Output:
  - silver.products
  - silver.customers
  - silver.orders

### ✅ Enriched Tables and Aggregated Tables : Gold Layer

- **Order Enriched Table**: Combines orders with:
  - Profit rounded to 2 decimal places
  - Customer name & country
  - Product category & subcategory

- Compute profit by:
  - Year
  - Product Category
  - Product Subcategory
  - Customer

Output:
  - gold.enriched_orders
  - gold.aggregated_profit

### ✅ SQL Aggregates (via Databricks SQL)
- Profit by Year
- Profit by Year + Product Category
- Profit by Customer
- Profit by Customer + Year

---

## Tech Stack

- **Platform**: Databricks (Azure)
- **Language**: Python 3, PySpark
- **Testing**: `pytest`
- **Orchestration**: Databricks Notebooks (modularized)

---

## 📁 Directory Structure

ecom-sales-pipeline/src<br>
├── etl/<br>
│   ├── data_extractor.py       # Logic to load raw datasets<br>
│   ├── data_standardizer.py    # Data cleaning and standardization<br>
│   ├── data_transformer.py     # Data enrichment and transformations<br>
│   └── constants.py            # Application-wide constants<br>
├── notebooks/<br>
│   └── ecom-sales.ipynb             # Main Databricks notebook entry point<br>
├── tests/<br>
│   ├── test_data_standardizer.py<br>
│   ├── test_data_transformer.py<br>
└── README.md                   # Project documentation<br>



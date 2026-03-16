# 🏪 Sales & Inventory Data Warehouse — Azure Data Engineering Project

![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?style=for-the-badge&logo=microsoftazure)
![ADF](https://img.shields.io/badge/Azure%20Data%20Factory-Pipelines-FF6D00?style=for-the-badge&logo=microsoftazure)
![Databricks](https://img.shields.io/badge/Azure%20Databricks-PySpark-FF3621?style=for-the-badge&logo=databricks)
![SQL](https://img.shields.io/badge/Azure%20SQL-Database-CC2927?style=for-the-badge&logo=microsoftsqlserver)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen?style=for-the-badge)

---

## 📋 Project Overview

A complete end-to-end **Azure Data Engineering** solution for a Sales & Inventory Data Warehouse. This project implements a full data pipeline that extracts data from SQL Server and flat file sources, transforms and validates it through multiple layers, and loads it into a star schema data warehouse for business intelligence and reporting.

---

## 🏗️ Architecture

```
Source Systems
    │
    ├── SQL Server (SALES DB)          ──→  ADF Pipeline  ──→  ADLS RAW Layer
    │   ORDER_HEADER, ORDER_DETAILS,                            (SQLSource/Target)
    │   PRODUCT, BRANCH, RETURN_ITEM,
    │   INVENTORY_LEVELS
    │
    └── Blob Storage (Flat Files)      ──→  ADF Pipeline  ──→  ADLS RAW Layer
        ORDER_METHOD, RETURN_REASON,                            (FileSource/Target)
        COUNTRY, WAREHOUSE,
        RETAILER, PRODUCT_NAME_LOOKUP
                                                │
                                                ▼
                                    Databricks Notebook
                                    (NB_RAW_TO_INGESTION)
                                    • Data Validation
                                    • Reject Handling
                                    • Archive Processing
                                                │
                                                ▼
                                    Azure SQL DB — Ingestion Layer
                                    (12 Staging Tables)
                                                │
                                                ▼
                                    Databricks Notebook
                                    (NB_INGESTION_TO_INTEGRATION)
                                    • SCD Type 1 & Type 2
                                    • Surrogate Key Generation
                                    • Fact Table Loading
                                                │
                                                ▼
                                    Azure SQL DB — Integration Layer
                                    (8 Dimension Tables + 2 Fact Tables)
```

---

## ☁️ Azure Services Used

| Service | Name | Purpose |
|---|---|---|
| Azure Data Factory | ADF-SalesProject-MK | Pipeline orchestration |
| Azure Databricks | ADB-SalesProject-MK | Data transformation (PySpark) |
| Azure Data Lake Storage Gen2 | adlssalesprojectmk | RAW data storage |
| Azure Blob Storage | blobstorageprojectmk | Source flat file landing zone |
| Azure SQL Database | ASQL-SalesProject-MK | Ingestion + Integration layers |
| Azure Key Vault | KV-SalesProject-MK | Secrets management |
| Azure Synapse Analytics | synapse-salesproject-mk | Analytics |

---

## 📁 Project Structure

```
├── ADF Pipelines/
│   ├── PL_EXT_SQL_SRC_TO_RAW_FULL      # SQL Source → ADLS RAW
│   ├── PL_EXT_FILE_SRC_TO_RAW          # Flat Files → ADLS RAW
│   ├── PL_INT_RAW_TO_INGESTION         # RAW → Ingestion (Databricks)
│   ├── PL_INT_INGESTION_TO_INTEGRATION # Ingestion → Integration (Databricks)
│   ├── PL_MASTER_SQL_SRC               # Master pipeline — SQL source
│   └── PL_MASTER_FILE_SRC              # Master pipeline — File source
│
├── Databricks Notebooks/
│   ├── NB_RAW_TO_INGESTION             # Validation + ingestion load
│   └── NB_INGESTION_TO_INTEGRATION     # SCD1/SCD2 + Fact table load
│
├── SQL Scripts/
│   ├── Schemas (ingestion, integration, control)
│   ├── Ingestion Layer Tables (12 tables)
│   ├── Integration Layer Tables (8 dims + 2 facts)
│   └── Control Tables (Watermark, TableLog, Error_Logging)
│
└── Unit Testing/
    └── Unit_Testing_Template_Filled.xlsx
```

---

## 🔄 Data Pipeline Flow

### Layer 1 — RAW Layer (ADLS Gen2)
- Data extracted from SQL Server and Blob Storage
- Files stored as CSV in timestamped format: `TableName_YYYYMMDDHHMMSS.csv`
- Folder structure: `RAW/SourceName/Target/`, `Archive/`, `Failure/`, `Reject/`
- Incremental load for `ORDER_HEADER` and `ORDER_DETAILS` using watermark table
- Full load for all other tables

### Layer 2 — Ingestion Layer (Azure SQL DB)
- Data validated and loaded from RAW layer using Databricks PySpark
- Validations applied per LLD document:
  - Reject null primary keys
  - Reject duplicate primary keys
  - Reject null/zero/negative values for quantity and price columns
- Rejected records saved as Parquet in RAW Reject folder
- Processed files moved to Archive folder

### Layer 3 — Integration Layer (Azure SQL DB)
- SCD Type 1 for lookup dimensions (7 tables)
- SCD Type 2 for `TBL_DIM_ORDER` with full version history
- Surrogate keys generated via SQL IDENTITY columns
- Fact tables loaded with all dimension SKs joined

---

## 📊 Data Model

### Dimension Tables
| Table | Type | Records |
|---|---|---|
| TBL_DIM_ORDER | SCD2 | 53,267 |
| TBL_DIM_PRODUCT | SCD1 | 274 |
| TBL_DIM_ORDER_METHOD_LKP | SCD1 | 7 |
| TBL_DIM_RETURN_REASON_LKP | SCD1 | 5 |
| TBL_DIM_COUNTRY_LKP | SCD1 | 21 |
| TBL_DIM_WAREHOUSE_LKP | SCD1 | 29 |
| TBL_DIM_RETAILER_LKP | SCD1 | 820 |
| TBL_DIM_PRODUCT_NAME_LKP | SCD1 | 274 |

### Fact Tables
| Table | Type | Records |
|---|---|---|
| TBL_FACT_SALES | Transactional | 446,023 |
| TBL_FACT_INVENTORY | Transactional | 145,960 |

---

## ⚙️ Key Features

### Dynamic Pipeline Design
- **Config table driven** — `control.PipelineConfig` drives all pipeline logic
- **Parameterized datasets** — single dataset handles all tables dynamically
- **ForEach loops** — one pipeline processes all tables automatically
- Adding a new table requires only one row in config table — no pipeline changes needed

### Watermark-based Incremental Loading
- `control.Watermark` table tracks last fetched timestamp
- `ORDER_HEADER` and `ORDER_DETAILS` load only new/changed records
- Watermark updated only after successful pipeline completion

### Audit Logging
- `control.TableLog` — tracks every pipeline execution with source/target counts
- `control.Error_Logging` — captures detailed error information on failure
- Stored procedures: `usp_InsertTableLog`, `usp_InsertErrorLog`, `usp_UpdateWatermark`

### SCD Type 2 Implementation
```
Day 1: Record P1 inserted → VERSION_NUM=1, END_EFFECTIVE_DATE=2099-12-31
Day 2: Record P1 changes  → Old record expired (END_EFFECTIVE_DATE=today)
                           → New record inserted with VERSION_NUM=2
```

---

## 🔐 Security

| Feature | Implementation |
|---|---|
| Dynamic Data Masking | UNIT_COST and UNIT_SALE_PRICE masked in TBL_FACT_SALES |
| Row Level Security | WareHouseFilter policy on TBL_DIM_WAREHOUSE_LKP |
| Microsoft Defender for SQL | Enabled at subscription level |
| Key Vault | All secrets stored in Azure Key Vault — no hardcoded credentials |

---

## 📅 Scheduling & Monitoring

| Feature | Configuration |
|---|---|
| Schedule | Daily at 9:00 AM IST (UTC+5:30) |
| Trigger Name | TR_DAILY_9AM_IST |
| Max Execution Time | 40 minutes |
| Retry Policy | 3 retries with 60 seconds wait |
| Failure Alert | ALT_Pipeline_Failure — email notification on any pipeline failure |

---

## 🚀 How to Run

### Prerequisites
- Azure subscription with required services deployed
- Databricks cluster running (Mayur Kumbhar's Cluster)
- Source SALES database imported via SALES.bacpac

### Execution Steps

**Run SQL Source Pipeline (Daily):**
```
ADF → PL_MASTER_SQL_SRC → Debug/Trigger
```
This automatically runs:
1. `PL_EXT_SQL_SRC_TO_RAW_FULL` — extracts SQL data to RAW layer
2. `PL_INT_RAW_TO_INGESTION` — loads RAW to Ingestion via Databricks
3. `PL_INT_INGESTION_TO_INTEGRATION` — loads Ingestion to Integration via Databricks

**Run Flat File Pipeline (On Demand):**
```
ADF → PL_MASTER_FILE_SRC → Debug/Trigger
```
This automatically:
1. Waits 10 minutes for file availability
2. Checks if file exists in Blob Storage
3. If found: runs full flat file pipeline
4. If not found: fails with FILE_NOT_FOUND error and sends notification

### Monitor Results
```sql
-- Check pipeline logs
SELECT TOP 10 * FROM control.TableLog ORDER BY DW_INSERT_DT DESC

-- Check watermark
SELECT * FROM control.Watermark

-- Check errors
SELECT TOP 10 * FROM control.Error_Logging ORDER BY DW_INSERT_DT DESC

-- Verify record counts
SELECT 'ORDER_HEADER'   , COUNT(*) FROM ingestion.ORDER_HEADER     UNION ALL
SELECT 'ORDER_DETAILS'  , COUNT(*) FROM ingestion.ORDER_DETAILS    UNION ALL
SELECT 'FACT_SALES'     , COUNT(*) FROM integration.TBL_FACT_SALES UNION ALL
SELECT 'FACT_INVENTORY' , COUNT(*) FROM integration.TBL_FACT_INVENTORY
```

---

## 🧪 Unit Testing

All 27 test cases documented in `Unit_Testing_Template_Filled.xlsx` covering:
- All ADF pipelines
- Both Databricks notebooks
- Security features
- Scheduling and alerting
- Retry mechanism

**All test cases: ✅ PASS**

---

## 👨‍💻 Author

**Mayur Kumbhar**
- Project: Sales & Inventory Data Warehouse — Azure Data Engineering
- Institute: IntelBi
- Environment: DEV (`Sales_DataEnginnering_Project_MK_DEV`)
- Region: Central India

---

## 📄 Documents Reference

| Document | Purpose |
|---|---|
| Sales and inventory_FS_20240229.docx | Functional Specification |
| LLD_Source_To_ADLS(RAW) Layer_LOAD.xlsx | Source to RAW mapping |
| LLD_RAW To_Ingestion_Layer_LOAD.xlsx | RAW to Ingestion mapping |
| LLD_Ingestion_TO Integration Layer LOAD.xlsx | Ingestion to Integration mapping |
| Unit_Testing_Template_Filled.xlsx | Unit test results |

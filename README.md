
# 🚀 SQL Data Warehouse Mastery Journey

## 📖 **Project Description**  
This is my personal **end-to-end data warehousing project**—a structured, self-paced journey from raw data ingestion to actionable analytics. I'm building this to:  
✔ **Master** Medallion Architecture and ETL pipelines.  
✔ **Implement** industry best practices for data modeling.  
✔ **Deliver** business-ready datasets for reporting.  

---

## 📌 **Core Components**  
![Medallion Architecture](https://github.com/user-attachments/assets/f883bc6e-1f3d-48bc-9f24-879fa4c992ea)  

### 🌱 **Bronze Layer (Raw)** ✅  
- **Data Ingestion**:  
  - Load CSV/JSON files *as-is*.  
  - Preserve source fidelity with metadata.  
- **Tools**:  
  - `BULK INSERT` (SQL Server)  
  - File staging tables.  

---

### 🔧 **Silver Layer (Cleaned)** ✅
- **Transformations**:  
  - Schema validation, deduplication.  
  - Standardize formats (dates, gender, marital status).  
- **SQL Techniques**:  
  - `TRIM()`, `CASE WHEN`, `SUBSTRING() `.  

---

### 🧠 **Gold Layer (Business-Ready)** 🚧 *Planned*  
- **Star Schema**:  
  - Fact tables (e.g., `fact_sales`).  
  - Dimensions (e.g., `dim_customer`).  
- **Optimizations**:  
  - Columnstore indexes.  
  - Partitioning.  

---

## 🛠️ **Tech Stack**  
| Category       | Tools                |  
|----------------|----------------------|  
| **Database**   | SQL Server Express   |  
| **ETL**        | T-SQL Stored Procs   |  
| **IDE** | SSMS (SQL Server Management Studio) |
|  **Version Control** | Git/GitHub |
|  **Documentation** | DrawIO (Architecture), Notion |
| **Orchestration** | SSIS (Future)      |  
| **Viz**        | Power BI (Future)    |  
---

## 🎯 **Goals**  
- Build a **production-like** data warehouse.  
- Practice **slowly changing dimensions (SCD)**.  
- Generate **executive dashboards**.  

---


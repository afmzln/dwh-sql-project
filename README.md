
# **SQL Data Warehouse Project**
A data warehousing and analytics solution showcasing end-to-end data engineering—from building a scalable data warehouse to deriving actionable insights. Highlights industry best practices in data integration, transformation, and visualization.

# 🏗️**Project Overview**

Build a **SQL Data Warehouse** with Medallion Architecture and ETL pipelines to transform raw data into business intelligence-ready datasets for reporting and analytics.

Key components:

1. **Data Architecture** – Implements a **Medallion Architecture** (Bronze, Silver, Gold layers) to organize data from raw to cleaned to business-ready states.
2. **ETL Pipeline** – Extracts, transforms, and loads data from source files (CSV) into the warehouse.
3. **Data Modeling** – Employs Star Schema to structure data for optimal reporting performance.
4. **Analytics & Reporting** – Enables business insights through dashboards and queries.

# 🏗️**Data Architecture (Medallion Layers)**

![MEDALLION LAYERS.drawio.png](attachment:af3d4c9d-de5b-4c49-9e83-98567a56a023:MEDALLION_LAYERS.drawio.png)

| Layer | Description | Key Action |
|-------|-------------|------------|
| **Bronze** | Raw, unprocessed data  | CSV ingestion to SQL Server |
| **Silver** | Cleaned/validated data | Deduplication,standardization |
| **Gold** | Business-ready data | Structured for analytics/reporting  |

# 🛠️**Tools Used**
| Category | Tools |
|----------|-------|
| Database | SQL Server Express |
| IDE | SSMS (SQL Server Management Studio) |
| Version Control | Git/GitHub |
| Documentation | DrawIO (Architecture), Notion |


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

![Image](https://github.com/user-attachments/assets/f883bc6e-1f3d-48bc-9f24-879fa4c992ea)
- **Bronze**: Stores raw data exactly as ingested (like CSVs), maintaining original fidelity.
- **Silver**: Filters, cleans, and standardizes data ensures reliable, consistent data for analysis.
- **Gold**: Business-friendly datasets structured for analytics (e.g., Star Schema)  enable rapid querying for reports and dashboards.

# 🛠️**Tools Used**
| Category | Tools |
|----------|-------|
| Database | SQL Server Express |
| IDE | SSMS (SQL Server Management Studio) |
| Version Control | Git/GitHub |
| Documentation | DrawIO (Architecture), Notion |

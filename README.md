
# **SQL Data Warehouse Project**

# 🏗️**Project Overview**

This project focuses on building a **SQL Server Data Warehouse** with ETL pipelines that transform raw data into **business intelligence-ready datasets** for analytics and reporting.

Key components:

1. **Data Architecture** – Implements a **Medallion Architecture** (Bronze, Silver, Gold layers) to organize data from raw to cleaned to business-ready states.
2. **ETL Pipeline** – Extracts, transforms, and loads data from source files (CSV) into the warehouse.
3. **Data Modeling** – Employs Star Schema to structure data for optimal reporting performance.
4. **Analytics & Reporting** – Enables business insights through dashboards and queries.

# 🏗️**Data Architecture (Medallion Layers)**

![MEDALLION LAYERS.drawio.png](attachment:af3d4c9d-de5b-4c49-9e83-98567a56a023:MEDALLION_LAYERS.drawio.png)

- **Bronze**: Stores raw data exactly as ingested (like CSVs), maintaining original fidelity.
- **Silver**: Filters, cleans, and standardizes data ensures reliable, consistent data for analysis.
- **Gold**: Business-friendly datasets structured for analytics (e.g., Star Schema)  enable rapid querying for reports and dashboards.

# 🛠️**Tools Used**

- **SQL Server Express**: Database storage.
- **SSMS**: Manage SQL Server databases.
- **Git**: Version control for scripts.
- **DrawIO**: Diagrams for architecture.
- **Notion**: Project documentation.

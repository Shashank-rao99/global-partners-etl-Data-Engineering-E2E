# Global Partners Data Engineering Pipeline

## Overview

This project implements a full end-to-end data engineering and analytics pipeline for restaurant order data. Using AWS cloud services, PySpark ETL, Amazon Athena, and a Streamlit dashboard, the project demonstrates scalable data ingestion, transformation, metric calculation, and business insights visualization.

---

## Architecture

- **Source:** SQL Server (RDS)
- **ETL/Processing:** AWS Glue with PySpark (`etl_scripts/globalpartners_etl.py`)
- **Storage:** Amazon S3 (Parquet)
- **Data Catalog:** AWS Glue Data Catalog
- **Query/Analytics:** Amazon Athena
- **Dashboard:** Streamlit (`dashboard/dashboard.py`) running locally, querying Athena
- **Orchestration:** AWS Glue jobs scheduled daily (batch)
- **Code & Docs:** This GitHub repository

*See `architecture_diagram` for full architectural details.*

---

## Repository Structure

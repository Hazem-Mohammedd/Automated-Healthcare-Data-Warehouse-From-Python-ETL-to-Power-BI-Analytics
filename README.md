# Automated-Healthcare-Data-Warehouse-From-Python-ETL-to-Power-BI-Analytics
Architected an automated Python-to-SQL data warehouse, eliminating clinical anomalies and manual ETL. Designed a high-performance Star Schema and dynamic Power BI dashboard with geospatial tracking, empowering leaders to monitor capacity, optimize patient flow, and track millions in revenue.

# Project Excerpt
Architected an end-to-end healthcare data warehouse. Built a Python/Streamlit ETL pipeline with automated anomaly resolution, designed a SQL Server Star Schema, and deployed an optimized Power BI model for facility-wide reporting.

# Business Case
Healthcare networks consistently struggle with fragmented, messy data—erratic physician titles, duplicate billing events, and conflicting patient records. This poor data quality breaks downstream BI tools, preventing hospital leadership from accurately tracking Length of Stay (LOS), revenue generation, and facility capacity.

# Project Goal
To engineer a fully automated, idempotent data pipeline that ingests raw hospital admission data, autonomously resolves clinical data anomalies, and feeds a clean, dimensional Star Schema optimized for high-performance enterprise reporting.

# Process & Methodology
Phase 1: Python ETL & User Interface: Built a custom Streamlit web application allowing users to securely upload raw CSV batches. Developed a Pandas-based transformation layer using Regular Expressions to standardize messy text (e.g., stripping erratic "Dr." and "Mr." prefixes) and enforce strict DateTime parsing.

Phase 2: Automated Anomaly Resolution: Identified and programmatically resolved the "Aging Patient Anomaly" (where raw data showed the same patient with conflicting ages on the same admission date) by aggregating duplicate events and dynamically preserving the minimum age.

Phase 3: Idempotent SQL Server Architecture: Engineered a SHA-256 composite business key (SourceAdmissionID) to prevent duplicate data loads. Architected a SQL Server Star Schema and deployed transactional T-SQL Stored Procedures to handle incremental MERGE loads, complete with "-1 (Unknown)" default members to protect referential integrity.

Phase 4: Advanced Power BI Modeling: Connected the SQL Data Warehouse to Power BI. Engineered a highly optimized, dynamically buffered Calendar table using Advanced M-Query to enable rapid, robust time-intelligence slicing across admission and discharge dates.

# Key Insights & Architectural Wins
Geospatial Optimization: Successfully isolated hospital latitude and longitude coordinates into a dedicated DimHospitals table. This required custom SQL grouping logic to prevent duplicate coordinate errors, ultimately keeping the FactAdmissions table lean while enabling precise map visuals.

Pre-Computed Performance: Embedded LengthOfStay calculations directly into the SQL Fact table as a persisted computed column, shifting the processing burden away from Power BI's DAX engine to ensure lightning-fast dashboard rendering.

# Impact
Eliminated Manual Data Prep: Reduced data ingestion and cleaning time from manual hours to a one-click, 5-second process via the Streamlit UI.

Guaranteed Data Integrity: Eradicated 100% of primary key violations and logical data conflicts before they could reach the production data warehouse.

Enterprise BI Readiness: Delivered a highly performant, structured foundation primed for executive dashboards, allowing stakeholders to slice billing and capacity metrics by dynamic time periods, geographic locations, and patient demographics.

# Tools & Technologies Used
Data Engineering & UI: Python (Pandas, Hashlib, Regular Expressions), Streamlit, SQLAlchemy, PyODBC

Database Architecture: Microsoft SQL Server (T-SQL, Stored Procedures, Star Schema Dimensional Modeling)

Business Intelligence: Power BI (Data Modeling, Advanced M-Query, DAX)

# Power BI Dashboard
Link: https://mavenshowcase.com/project/54488

# **US Corn Market Risk & Supply Intelligence**
**Overview**
This project implements an end-to-end data analytics pipeline to analyze US corn production, yield, harvested area, and market prices using official USDA datasets.
The focus is on transforming raw agricultural data into business-ready insights that help understand price risk, supply concentration, and yield efficiency over time.

The solution follows a Lakehouse architecture (Bronze → Silver → Gold) built on Databricks, with automated jobs and interactive SQL dashboards.

**Problem Statement or business use cases**

- Corn prices and supply vary significantly year over year due to changes in:
- 
- Yield efficiency
- 
- Production volumes
- 
- Area harvested
- 
- Regional concentration
- 
- This project answers questions such as:
- 
- How have corn prices changed over time?
- 
- Which states contribute most to total production?
- 
- Is yield efficiency improving or declining?
- 
- Where is production risk concentrated?

**Architecture**
USDA QuickStats API
        ↓
Bronze Layer (Raw ingestion)
        ↓
Silver Layer (Cleaned & standardized)
        ↓
Gold Layer (Aggregated KPIs)
        ↓
Databricks SQL Dashboards

### Data Layers
**Bronze Layer**

Raw data ingested directly from the USDA QuickStats API

Stored as Delta files

Minimal transformation

One dataset per statistic:
- Yield

- Production

- Area Harvested

- Price Received

**Silver Layer**

Cleaned and standardized datasets

Applied schema normalization and type casting

Removed inconsistencies and null handling
- - - - - 
Separate tables for each metric:
- silver_yield

- silver_production

- silver_area_harvested

- silver_price_received

**Gold Layer**

Business-ready datasets optimized for analytics and dashboards.

Gold tables include:

- **usda_corn_gold**
Year-level aggregates combining yield, production, area, and price

- **usda_corn_gold_state_analysis**
State-wise yearly performance metrics

- **usda_corn_state_risk**
Production and price volatility by state

- **usda_corn_yield_efficiency**
Yield efficiency (bushels per acre)

**Key Metrics & KPIs**

- Average corn price trend (YoY)
- 
- Total production by year
- 
- Yield efficiency (BU / ACRE)
- 
- Top producing states
- 
- Production concentration by state
- 
- Price and production volatility indicators

**Dashboards**

- Interactive dashboards built using Databricks SQL:
- 
- Line charts for price and yield trends
- 
- Bar charts for top producing states
- 
- KPI cards for key metrics
- 
- Filters for year and state selection
- 
- Dashboard design focuses on readability and decision-making, not just visualization.

**Automation**

- Databricks Jobs orchestrate the full pipeline
- 
- Gold layer execution depends on successful Silver layer completion
- 
- Enables repeatable and reliable data refresh

**Tech Stack**

- Databricks (Community Edition)
- 
- Apache Spark (PySpark)
- 
- Delta Lake
- 
- Databricks SQL
- 
- USDA QuickStats API
-- ============================================================================
-- Spencer's CDP Data Warehouse - Schema Creation
-- ============================================================================

-- Bronze layer: raw ingested data
CREATE SCHEMA IF NOT EXISTS bronze;

-- Staging layer: dbt staging views
CREATE SCHEMA IF NOT EXISTS staging;

-- Silver layer: cleaned, conformed dimensions and facts (DWH core)
CREATE SCHEMA IF NOT EXISTS silver;

-- Silver-Identity layer: identity resolution, identity graphs
CREATE SCHEMA IF NOT EXISTS silver_identity;

-- Silver-Gold layer: aggregated business views, NOB, sales summaries
CREATE SCHEMA IF NOT EXISTS silver_gold;

-- Reverse ETL layer: derived attributes, audience syncs
CREATE SCHEMA IF NOT EXISTS silver_reverse_etl;

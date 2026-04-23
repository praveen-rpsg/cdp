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
CREATE SCHEMA IF NOT EXISTS identity;

-- Silver-Gold layer: aggregated business views, NOB, sales summaries
CREATE SCHEMA IF NOT EXISTS silver_gold;
CREATE SCHEMA IF NOT EXISTS gold;

-- Reverse ETL layer: derived attributes, audience syncs
CREATE SCHEMA IF NOT EXISTS silver_reverse_etl;
CREATE SCHEMA IF NOT EXISTS reverse_etl;

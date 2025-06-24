-- =========================================================================
-- Bronze Layer: External Tables (Raw Data from S3)
-- =========================================================================
-- This script creates external tables that directly read from S3 Parquet files
-- These tables form the Bronze layer of the medallion architecture

USE DATABASE ${SNOWFLAKE_DATABASE};
USE SCHEMA ${BRONZE_LAYER};

-- -------------------------------------------------------------------------
-- Bronze Table 1: Raw Users Data
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ext_raw_users (
    id STRING AS (value:id::STRING),
    first_name STRING AS (value:first_name::STRING),
    last_name STRING AS (value:last_name::STRING),
    gender STRING AS (value:gender::STRING),
    address STRING AS (value:address::STRING),
    post_code STRING AS (value:post_code::STRING),
    email STRING AS (value:email::STRING),
    username STRING AS (value:username::STRING),
    dob STRING AS (value:dob::STRING),
    registered_date STRING AS (value:registered_date::STRING),
    phone STRING AS (value:phone::STRING),
    picture STRING AS (value:picture::STRING)
)
LOCATION = @data_lake_stage/users/raw/parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Bronze Table 2: User Analytics Data
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ext_user_analytics (
    gender STRING AS (value:gender::STRING),
    count NUMBER AS (value:count::NUMBER),
    percentage NUMBER AS (value:percentage::NUMBER),
    email_domain STRING AS (value:email_domain::STRING),
    domain_count NUMBER AS (value:domain_count::NUMBER),
    domain_percentage NUMBER AS (value:domain_percentage::NUMBER),
    processing_timestamp TIMESTAMP AS (value:processing_timestamp::TIMESTAMP)
)
LOCATION = @data_lake_stage/users/analytics/parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Bronze Table 3: User Demographics Data
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ext_user_demographics (
    country STRING AS (value:country::STRING),
    state STRING AS (value:state::STRING),
    user_count NUMBER AS (value:user_count::NUMBER),
    avg_age NUMBER AS (value:avg_age::NUMBER),
    male_percentage NUMBER AS (value:male_percentage::NUMBER),
    female_percentage NUMBER AS (value:female_percentage::NUMBER),
    processing_timestamp TIMESTAMP AS (value:processing_timestamp::TIMESTAMP)
)
LOCATION = @data_lake_stage/users/demographics/parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- Examine created external tables
SHOW EXTERNAL TABLES;
DESCRIBE TABLE ext_raw_users;
DESCRIBE TABLE ext_user_analytics;
DESCRIBE TABLE ext_user_demographics; 
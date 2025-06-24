-- Snowflake External Tables for Pipeline Data
-- This script creates external tables that automatically read from S3 Parquet files

USE DATABASE ECOMMERCE_DB;
USE SCHEMA STAGING;

-- External table for raw user data
CREATE EXTERNAL TABLE IF NOT EXISTS ext_raw_users (
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
WITH LOCATION = @raw_users_stage
AUTO_REFRESH = TRUE
FILE_FORMAT = parquet_format
COMMENT = 'External table for raw user data from RandomUser API via Kafka pipeline';

-- External table for user analytics
CREATE EXTERNAL TABLE IF NOT EXISTS ext_user_analytics (
  metric_type STRING AS (value:metric_type::STRING),
  metric_name STRING AS (value:metric_name::STRING),
  metric_value NUMBER AS (value:metric_value::NUMBER),
  percentage FLOAT AS (value:percentage::FLOAT),
  created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
WITH LOCATION = @user_analytics_stage
AUTO_REFRESH = TRUE
FILE_FORMAT = parquet_format
COMMENT = 'External table for user analytics (gender distribution, email domains, general metrics)';

-- External table for user demographics
CREATE EXTERNAL TABLE IF NOT EXISTS ext_user_demographics (
  demographic_type STRING AS (value:demographic_type::STRING),
  demographic_value STRING AS (value:demographic_value::STRING),
  user_count NUMBER AS (value:user_count::NUMBER),
  percentage FLOAT AS (value:percentage::FLOAT),
  created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
WITH LOCATION = @user_demographics_stage
AUTO_REFRESH = TRUE
FILE_FORMAT = parquet_format
COMMENT = 'External table for user demographics (country and state distribution)';

-- Examine created external tables
SHOW EXTERNAL TABLES;
DESCRIBE TABLE ext_raw_users;
DESCRIBE TABLE ext_user_analytics;
DESCRIBE TABLE ext_user_demographics; 
-- ========================================================================
-- Snowflake Storage Integration and Stage Setup
-- ========================================================================
-- This script sets up the S3 storage integration and stages for Iceberg tables
-- Environment variables will be substituted during CI/CD deployment

-- Create storage integration for S3 access (Iceberg warehouse)
CREATE STORAGE INTEGRATION iceberg_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://${S3_BUCKET_NAME}/iceberg-warehouse/');

-- Describe the integration to get the external ID for AWS role trust policy
-- Note: Run this manually to get the STORAGE_AWS_EXTERNAL_ID for your AWS role trust policy
DESC STORAGE INTEGRATION s3_iceberg_integration;

-- Create external volume and external catalog

CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_VOL
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-primary'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://${S3_BUCKET_NAME}/iceberg-warehouse/'
            STORAGE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
            STORAGE_AWS_EXTERNAL_ID = '${STORAGE_AWS_EXTERNAL_ID}'
         )
      )
      ALLOW_WRITES = TRUE;

-- Create catalog integration for AWS Glue
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_integration
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'data_pipeline_db'
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE
  GLUE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
  GLUE_CATALOG_ID = '${AWS_ACCOUNT_ID}'
  GLUE_REGION = '${AWS_REGION}';


CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
USE DATABASE ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS BRONZE_LAYER;
USE SCHEMA BRONZE_LAYER;

-- Create stages for each data type
CREATE STAGE IF NOT EXISTS raw_users_stage
  URL = 's3://my-amazing-app/users/raw/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}'
    AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for raw user data from Kafka pipeline';

CREATE STAGE IF NOT EXISTS user_analytics_stage
  URL = 's3://my-amazing-app/users/analytics/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}' 
    AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for user analytics data (gender, email domains, etc.)';

CREATE STAGE IF NOT EXISTS user_demographics_stage
  URL = 's3://my-amazing-app/users/demographics/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}'
    AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for user demographics data (countries, states)';

-- Test listing files in each stage
SHOW STAGES;

-- List files in each stage to verify connection
LIST @raw_users_stage;
LIST @user_analytics_stage;
LIST @user_demographics_stage; 
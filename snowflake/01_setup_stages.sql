-- =========================================================================
-- Snowflake Storage Integration and Stage Setup
-- =========================================================================
-- This script sets up the S3 storage integration and stages for Iceberg tables
-- Environment variables will be substituted during CI/CD deployment

-- Create storage integration for S3 access (Iceberg warehouse)
CREATE OR REPLACE STORAGE INTEGRATION s3_iceberg_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://${S3_BUCKET_NAME}/iceberg-warehouse/');

-- Describe the integration to get the external ID for AWS role trust policy
-- Note: Run this manually to get the STORAGE_AWS_EXTERNAL_ID for your AWS role trust policy
DESC STORAGE INTEGRATION s3_iceberg_integration;

-- Create stage for Iceberg tables
CREATE OR REPLACE STAGE iceberg_stage
  STORAGE_INTEGRATION = s3_iceberg_integration
  URL = 's3://${S3_BUCKET_NAME}/iceberg-warehouse/'
  FILE_FORMAT = (TYPE = PARQUET);

-- Test the stage (uncomment to verify setup)
-- LIST @iceberg_stage;

-- Snowflake Setup: Create Stages for S3 Parquet Files
-- This script sets up external stages to read Parquet files from S3

-- Set AWS credentials (replace with your actual credentials)
-- You can also set these as Snowflake variables or use environment variables
-- Create database and schema if they don't exist

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
USE DATABASE ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Create stages for each data type
CREATE OR REPLACE STAGE raw_users_stage
  URL = 's3://my-amazing-app/users/raw/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}'
    AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for raw user data from Kafka pipeline';

CREATE OR REPLACE STAGE user_analytics_stage
  URL = 's3://my-amazing-app/users/analytics/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = '${AWS_ACCESS_KEY_ID}' 
    AWS_SECRET_KEY = '${AWS_SECRET_ACCESS_KEY}'
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for user analytics data (gender, email domains, etc.)';

CREATE OR REPLACE STAGE user_demographics_stage
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
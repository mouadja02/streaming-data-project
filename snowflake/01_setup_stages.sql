-- Snowflake Setup: Create Stages for S3 Parquet Files
-- This script sets up external stages to read Parquet files from S3

-- Set AWS credentials (replace with your actual credentials)
-- You can also set these as Snowflake variables or use environment variables
SET (YOUR_ACCESS_KEY_ID, YOUR_SECRET_ACCESS_KEY) = ('${AWS_ACCESS_KEY_ID}', '${AWS_SECRET_ACCESS_KEY}');

-- Create database and schema if they don't exist
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DATABASE;
USE DATABASE ECOMMERCE_DATABASE;
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Create stages for each data type
CREATE STAGE IF NOT EXISTS raw_users_stage
  URL = 's3://my-amazing-app/users/raw/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = $YOUR_ACCESS_KEY_ID
    AWS_SECRET_KEY = $YOUR_SECRET_ACCESS_KEY
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for raw user data from Kafka pipeline';

CREATE STAGE IF NOT EXISTS user_analytics_stage
  URL = 's3://my-amazing-app/users/analytics/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = $YOUR_ACCESS_KEY_ID 
    AWS_SECRET_KEY = $YOUR_SECRET_ACCESS_KEY
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for user analytics data (gender, email domains, etc.)';

CREATE STAGE IF NOT EXISTS user_demographics_stage
  URL = 's3://my-amazing-app/users/demographics/parquet/'
  CREDENTIALS = (
    AWS_KEY_ID = $YOUR_ACCESS_KEY_ID
    AWS_SECRET_KEY = $YOUR_SECRET_ACCESS_KEY
  )
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')
  COMMENT = 'Stage for user demographics data (countries, states)';

-- Test listing files in each stage
SHOW STAGES;

-- List files in each stage to verify connection
LIST @raw_users_stage;
LIST @user_analytics_stage;
LIST @user_demographics_stage; 
-- Snowflake File Formats for Parquet Data
-- This script creates reusable file formats for different data types

USE DATABASE ECOMMERCE_DB;
USE SCHEMA STAGING;

-- Standard Parquet format with Snappy compression
CREATE FILE FORMAT IF NOT EXISTS parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY'
  COMMENT = 'Standard Parquet format with Snappy compression for pipeline data';

-- Alternative Parquet format with different settings (if needed)
CREATE FILE FORMAT IF NOT EXISTS parquet_format_uncompressed
  TYPE = 'PARQUET'
  COMPRESSION = 'NONE'
  COMMENT = 'Uncompressed Parquet format for debugging';

-- Show created file formats
SHOW FILE FORMATS; 
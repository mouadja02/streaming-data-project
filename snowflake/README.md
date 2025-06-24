# Snowflake Integration for Data Pipeline

This directory contains SQL scripts and Python utilities to connect your real-time data pipeline to Snowflake for advanced analytics and visualization.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [SQL Scripts](#sql-scripts)
- [Python Integration](#python-integration)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)

## Overview

The Snowflake integration provides:
- **External Tables** that automatically read Parquet files from S3
- **Real-time Analytics** on user data from your Kafka pipeline
- **Sample Queries** for common analytics use cases
- **Python Connector** for automated setup and testing

### Architecture Flow
```
Kafka → Spark → S3 (Parquet) → Snowflake External Tables → Analytics
```

## Prerequisites

### 1. Snowflake Account
- Active Snowflake account with appropriate permissions
- Database, schema, and warehouse already created
- User with rights to create stages and external tables

### 2. AWS S3 Access
- S3 bucket with pipeline data (Parquet files)
- AWS credentials with S3 read permissions
- Same credentials used by your data pipeline

### 3. Python Dependencies
```bash
pip install snowflake-connector-python==3.7.0
```

## Setup Instructions

### Step 1: Configure Environment Variables

Add these variables to your `.env` file:

```bash
# Snowflake Configuration
SNOWFLAKE_USER=INTERNPROJECT
SNOWFLAKE_PASSWORD=your_password_here
SNOWFLAKE_ACCOUNT=your_account-INTERNPROJECT
SNOWFLAKE_WAREHOUSE=INT_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DATABASE
SNOWFLAKE_SCHEMA=STAGING

# AWS Credentials (same as pipeline)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=my-amazing-app
```

### Step 2: Run Automated Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set up Snowflake tables automatically
python snowflake_connector.py

# Or run individual SQL scripts manually in Snowflake UI
```

### Step 3: Verify Setup

```bash
# Test external tables
python snowflake_connector.py query
```

## SQL Scripts

### 1. `01_setup_stages.sql`
Creates external stages pointing to your S3 bucket locations:
- `raw_users_stage` - Raw user data from Kafka
- `user_analytics_stage` - Processed analytics (gender, email domains)
- `user_demographics_stage` - Geographic demographics

### 2. `02_create_file_formats.sql`
Defines Parquet file formats with Snappy compression for optimal performance.

### 3. `03_create_external_tables.sql`
Creates external tables that automatically map Parquet schema to Snowflake columns:
- `ext_raw_users` - Individual user records
- `ext_user_analytics` - Aggregated metrics
- `ext_user_demographics` - Geographic distributions

### 4. `04_sample_queries.sql`
Comprehensive analytics queries including:
- Gender distribution analysis
- Geographic user distribution
- Email domain analytics
- Age group analysis
- Data quality checks
- Pipeline monitoring queries

## Python Integration

### SnowflakeConnector Class

```python
from snowflake_connector import SnowflakeConnector

# Initialize and connect
connector = SnowflakeConnector()
connector.connect()

# Execute queries
results = connector.execute_query("SELECT COUNT(*) FROM ext_raw_users")

# Test all tables
test_results = connector.test_external_tables()
```

### Available Functions

- `setup_snowflake_tables()` - Automated setup of all stages and tables
- `run_sample_queries()` - Execute sample analytics queries
- `test_external_tables()` - Verify table connectivity and record counts

## Usage Examples

### Basic Analytics Queries

```sql
-- Total users processed
SELECT COUNT(*) as total_users FROM ext_raw_users;

-- Gender distribution
SELECT 
  metric_name,
  metric_value as count,
  percentage
FROM ext_user_analytics 
WHERE metric_type = 'gender_distribution'
ORDER BY metric_value DESC;

-- Top countries
SELECT 
  demographic_value as country,
  user_count,
  percentage
FROM ext_user_demographics
WHERE demographic_type = 'country_distribution'
ORDER BY user_count DESC
LIMIT 10;
```

### Advanced Analytics

```sql
-- Age distribution with custom groups
SELECT 
  CASE 
    WHEN age < 25 THEN '18-24'
    WHEN age < 35 THEN '25-34'
    WHEN age < 45 THEN '35-44'
    WHEN age < 55 THEN '45-54'
    WHEN age < 65 THEN '55-64'
    ELSE '65+'
  END as age_group,
  COUNT(*) as count
FROM (
  SELECT 
    DATEDIFF('year', TO_DATE(dob), CURRENT_DATE()) as age
  FROM ext_raw_users
) 
GROUP BY age_group
ORDER BY age_group;
```

### Data Quality Monitoring

```sql
-- Check for data issues
SELECT 
  'Missing emails' as check_type,
  COUNT(*) as count
FROM ext_raw_users 
WHERE email IS NULL OR email = ''

UNION ALL

SELECT 
  'Invalid email format' as check_type,
  COUNT(*) as count
FROM ext_raw_users 
WHERE email NOT LIKE '%@%.%';
```

## Troubleshooting

### Common Issues

#### 1. **Connection Failed**
```
❌ Failed to connect to Snowflake: 250001 (08001): Failed to connect
```
**Solution:**
- Verify Snowflake credentials in `.env`
- Check account identifier format: `account-organization`
- Ensure warehouse is running and accessible

#### 2. **External Table Empty**
```
✅ ext_raw_users: 0 records
```
**Solutions:**
- Verify S3 bucket has Parquet files: `LIST @raw_users_stage;`
- Check AWS credentials have S3 read permissions
- Ensure pipeline has uploaded data to S3
- Verify S3 path matches stage URL

#### 3. **Permission Denied**
```
SQL access control error: Insufficient privileges
```
**Solutions:**
- Grant `CREATE STAGE` privilege to user
- Grant `CREATE TABLE` privilege to user
- Ensure user has `USAGE` on warehouse and database

#### 4. **File Format Issues**
```
Error parsing Parquet file
```
**Solutions:**
- Verify Parquet files are valid: check pipeline logs
- Ensure Snappy compression is used
- Check schema compatibility between Spark and Snowflake

### Debugging Commands

```sql
-- Check stages
SHOW STAGES;
LIST @raw_users_stage;

-- Check external tables
SHOW EXTERNAL TABLES;
DESCRIBE TABLE ext_raw_users;

-- Test file access
SELECT $1 FROM @raw_users_stage LIMIT 1;

-- Check recent errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%ext_raw_users%'
ORDER BY START_TIME DESC
LIMIT 5;
```

### Performance Optimization

1. **Clustering Keys** (for large datasets):
```sql
ALTER TABLE ext_raw_users CLUSTER BY (registered_date);
```

2. **Materialized Views** (for frequently accessed aggregations):
```sql
CREATE MATERIALIZED VIEW mv_user_summary AS
SELECT 
  gender,
  COUNT(*) as count,
  AVG(DATEDIFF('year', TO_DATE(dob), CURRENT_DATE())) as avg_age
FROM ext_raw_users
GROUP BY gender;
```

3. **Result Caching**: Snowflake automatically caches query results for 24 hours.

## Monitoring and Alerts

### Pipeline Health Monitoring

```sql
-- Monitor data freshness
SELECT 
  'Raw Data' as data_type,
  MAX(TO_TIMESTAMP(registered_date)) as latest_record,
  COUNT(*) as total_records
FROM ext_raw_users

UNION ALL

SELECT 
  'Analytics' as data_type,
  MAX(created_at) as latest_record,
  COUNT(*) as total_records
FROM ext_user_analytics;
```

### Set Up Alerts

Create Snowflake tasks to monitor data pipeline health:

```sql
-- Create task to check data freshness daily
CREATE OR REPLACE TASK pipeline_health_check
  WAREHOUSE = INT_WH
  SCHEDULE = 'USING CRON 0 9 * * * UTC'  -- Daily at 9 AM UTC
AS
SELECT 
  CASE 
    WHEN MAX(TO_TIMESTAMP(registered_date)) < DATEADD('hour', -25, CURRENT_TIMESTAMP())
    THEN 'ALERT: Data pipeline may be down'
    ELSE 'OK: Data pipeline healthy'
  END as status
FROM ext_raw_users;
```

## Next Steps

1. **Visualization**: Connect Snowflake to Tableau, Power BI, or Looker
2. **Machine Learning**: Use Snowpark for advanced analytics
3. **Data Sharing**: Set up secure data sharing with partners
4. **Automation**: Create stored procedures for regular data processing
5. **Scaling**: Implement auto-scaling warehouses for varying workloads
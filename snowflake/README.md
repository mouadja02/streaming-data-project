# Snowflake Integration - Medallion Architecture

This directory contains SQL scripts for implementing a **Medallion Architecture** in Snowflake with Bronze and Silver layers for your data pipeline.

## 🏗️ Architecture Overview

```
API → Kafka → Spark → S3 → AWS Glue → Iceberg → Snowflake
                     ↓
              Bronze Layer (Raw)
                     ↓
              Silver Layer (Processed)
                     ↓
              Analytics Queries
```

### Medallion Layers

**🟤 Bronze Layer** - Raw data directly from S3:
- `ext_raw_users` - Raw user data from RandomUser API
- `ext_user_analytics` - Basic Spark analytics  
- `ext_user_demographics` - Geographic data

**🥈 Silver Layer** - Processed data from AWS Glue Jobs:
- `users_transformed_ext` - Enriched user data with quality scores
- `dim_user_demographics_ext` - User demographics dimension
- `fact_geographic_analysis_ext` - Geographic analysis
- `fact_age_generation_analysis_ext` - Age/generation insights
- `fact_email_provider_analysis_ext` - Email analytics
- `fact_data_quality_metrics_ext` - Quality monitoring
- Plus additional fact tables

## 📁 File Structure

```
snowflake/
├── 01_setup_stages.sql          # S3 storage integration & stages
├── 02_create_file_formats.sql   # Parquet file formats
├── 03_create_external_tables.sql # Bronze layer tables
├── 04_sample_queries.sql        # Legacy queries (Bronze layer)
├── 05_create_iceberg_tables.sql # Silver layer tables (CI/CD)
├── 06_analytics_queries.sql     # Analytics queries (User-run)
└── README.md                    # This documentation
```

## 🚀 Quick Setup

### Step 1: Configure Environment
Add to your `.env` file:
```bash
# Snowflake Configuration
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema


# AWS Role for Snowflake Integration
AWS_ROLE_ARN=arn:aws:iam::your_account_id:role/snowflake-s3-role
```

### Step 2: Deploy via CI/CD
The CI/CD pipeline automatically creates:
- ✅ Storage integration and stages
- ✅ Bronze layer external tables
- ✅ Silver layer Iceberg tables

```bash
git add .
git commit -m "deploy: snowflake medallion architecture"
git push origin main
```

### Step 3: Run Analytics
After deployment, use the analytics queries:
```sql
-- Copy queries from 06_analytics_queries.sql
-- Replace ${SNOWFLAKE_DATABASE}, BRONZE_LAYER, and SILVER_LAYER with your actual values
-- Example: your_database.bronze_schema.table_name
```

## 📊 Usage Examples

### Bronze Layer (Raw Data Analysis)
```sql
-- Basic user overview
SELECT 
    COUNT(*) as total_users,
    COUNT(DISTINCT gender) as gender_types
FROM your_database.your_bronze_schema.ext_raw_users;

-- Geographic distribution
SELECT country, user_count, avg_age
FROM your_database.your_bronze_schema.ext_user_demographics
ORDER BY user_count DESC LIMIT 10;
```

### Silver Layer (Advanced Analytics)
```sql
-- Data quality insights
SELECT 
    target_segment,
    COUNT(*) as user_count,
    AVG(data_quality_score) as avg_quality
FROM your_database.your_silver_schema.dim_user_demographics_ext
GROUP BY target_segment;

-- Email provider market share
SELECT 
    email_provider_type,
    total_users,
    ROUND(total_users * 100.0 / SUM(total_users) OVER(), 2) as market_share_pct
FROM your_database.your_silver_schema.fact_email_provider_analysis_ext
ORDER BY total_users DESC;
```

### Cross-Layer Analysis
```sql
-- Compare raw vs processed data
SELECT 
    'Bronze Layer' as layer,
    COUNT(*) as record_count
FROM your_database.your_bronze_schema.ext_raw_users

UNION ALL

SELECT 
    'Silver Layer' as layer,
    COUNT(*) as record_count
FROM your_database.your_silver_schema.users_transformed_ext;
```

## 🔧 Manual Setup (Alternative)

If not using CI/CD, run scripts manually in this order:

1. **Setup Storage** (Admin): `01_setup_stages.sql`
2. **File Formats**: `02_create_file_formats.sql`  
3. **Bronze Tables**: `03_create_external_tables.sql`
4. **Silver Tables**: `05_create_iceberg_tables.sql`
5. **Analytics**: `06_analytics_queries.sql`

## 📈 Available Analytics

### Demographic Analysis
- Target segment distribution
- Generation and age group insights
- Geographic diversity analysis
- Business vs personal email patterns

### Data Quality Monitoring
- Quality score distributions
- Email/phone validation rates
- Data completeness metrics
- Quality trends by segment

### Email Provider Insights
- Market share analysis
- Domain popularity trends
- Provider age demographics
- Geographic email patterns

### Geographic Analysis
- Top countries by user count
- Cities with business email concentration
- Regional diversity metrics
- Geographic expansion patterns

## 🛠️ Troubleshooting

### Common Issues

**Storage Integration Errors:**
```sql
-- Check integration status
DESC STORAGE INTEGRATION s3_iceberg_integration;

-- Test stage access
LIST @iceberg_stage;
```

**External Table Issues:**
```sql
-- Check table metadata
SHOW EXTERNAL TABLES;

-- Refresh external table
ALTER EXTERNAL TABLE table_name REFRESH;
```

**No Data in Tables:**
- Verify S3 bucket permissions
- Check AWS role trust policy
- Ensure Parquet files exist in S3
- Verify AUTO_REFRESH is enabled

### Performance Optimization

**For Large Datasets:**
- Use clustering keys on frequently queried columns
- Implement result caching for repeated queries
- Use appropriate warehouse sizes
- Consider materialized views for complex aggregations

## 🔐 Security Best Practices

- Use least-privilege AWS IAM roles
- Rotate Snowflake passwords regularly
- Enable MFA on Snowflake accounts
- Monitor external table access logs
- Use private endpoints when possible

## 📚 Additional Resources

- [Snowflake External Tables Documentation](https://docs.snowflake.com/en/user-guide/tables-external-intro.html)
- [AWS S3 Integration Guide](https://docs.snowflake.com/en/user-guide/data-load-s3.html)
- [Medallion Architecture Best Practices](https://databricks.com/glossary/medallion-architecture)

---

**Need Help?** Check the main project README or open an issue for support.
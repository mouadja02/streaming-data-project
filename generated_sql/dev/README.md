# Generated Snowflake SQL Files - DEV Environment

This directory contains the generated Snowflake SQL files for the **dev** environment.

## Environment Configuration

- **Database**: `ECOMMERCE_DB`
- **Warehouse**: `INT_WH`
- **S3 Bucket**: `my-amazing-app`
- **Generated**: 2025-06-25 21:32:22 UTC

## Files

1. **02_create_file_formats.sql** - File formats for Parquet data
2. **03_bronze_layer.sql** - External tables (Bronze layer)
3. **04_bronze_checks.sql** - Data validation and quality checks
4. **05_silver_layer.sql** - Iceberg tables (Silver layer)
5. **06_gold_layer.sql** - Analytics views (Gold layer)
6. **07_final_checks.sql** - Final validation and examples

## Execution Order

Execute the files in the order listed above. You can run them in Snowflake using:

```sql
-- 1. Set up file formats
@02_create_file_formats.sql

-- 2. Create bronze layer tables
@03_bronze_layer.sql

-- 3. Validate bronze layer data
@04_bronze_checks.sql

-- 4. Create silver layer tables
@05_silver_layer.sql

-- 5. Create gold layer views
@06_gold_layer.sql

-- 6. Run final checks and examples
@07_final_checks.sql
```

## Notes

- ⚠️  **Do not edit these files directly** - they are automatically generated
- Modify the template files in the `snowflake/` directory instead
- All variables have been replaced with environment-specific values
- No credentials are included in these files
- Re-generate files when templates change using: `python scripts/generate_snowflake_sql.py dev`

## Template Variables Replaced

The following variables were replaced in the templates:

| Variable | Value |
|----------|-------|
| `SNOWFLAKE_DATABASE` | `ECOMMERCE_DB` |
| `SNOWFLAKE_WAREHOUSE` | `INT_WH` |
| `S3_BUCKET_NAME` | `my-amazing-app` |

## Regeneration

To regenerate these files:

```bash
# From project root directory
python scripts/generate_snowflake_sql.py dev
```

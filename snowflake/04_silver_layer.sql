  -- -------------------------------------------------------------------------
-- Transformed Users Data (Iceberg Table)
-- -------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE users_transformed
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'users_cleaned_transformed'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Transformed users data from Glue Catalog';

select * from users_transformed ;

-- -------------------------------------------------------------------------
-- Data Quality Summary (Iceberg Table)
-- -------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE data_quality_summary
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'users_cleaned_quality_summary'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Check the data quality';

select * from data_quality_summary;
-- -------------------------------------------------------------------------
-- User Demographics Dimension (Iceberg Table)
-- -------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE dim_user_demographics
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'dim_user_demographics'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Demographics Dimension';

select * from dim_user_demographics;

-- -------------------------------------------------------------------------
-- Geographic Analysis Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_geographic_analysis
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_geographic_analysis'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Geographic Analysis Fact Table';

select * from fact_geographic_analysis;


-- -------------------------------------------------------------------------
-- Age/Generation Analysis Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_age_generation_analysis
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_age_generation_analysis'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Age/Generation Analysis Fact Table';

select * from fact_age_generation_analysis;


-- -------------------------------------------------------------------------
-- Email Provider Analysis Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_email_provider_analysis
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_email_provider_analysis'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Email Provider Analysis Fact Table';

select * from fact_email_provider_analysis;


-- -------------------------------------------------------------------------
-- Email Domain Analysis Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_email_domain_analysis
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_email_domain_analysis'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Email Domain Analysis Fact Table';

select * from fact_email_domain_analysis;

-- -------------------------------------------------------------------------
-- Data Quality Metrics Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_data_quality_metrics
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_data_quality_metrics'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Data Quality Metrics Fact Table';

select * from fact_data_quality_metrics;

-- -------------------------------------------------------------------------
-- Quality by Segment Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------


CREATE OR REPLACE ICEBERG TABLE fact_quality_by_segment
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_quality_by_segment'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Quality by Segment Fact Table';

select * from fact_quality_by_segment;

-- =========================================================================
-- TIME SERIES ANALYSIS TABLES (JOB 3)
-- =========================================================================

-- -------------------------------------------------------------------------
-- Registration Trends Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_registration_trends
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_registration_trends'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Registration trends with time-based analysis (monthly, quarterly, day of week)';

select * from fact_registration_trends;

-- -------------------------------------------------------------------------
-- Geographic Expansion Trends Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_geographic_expansion
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_geographic_expansion'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Geographic expansion patterns and country penetration metrics';

select * from fact_geographic_expansion;

-- -------------------------------------------------------------------------
-- Age Demographic Trends Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_age_demographic_trends
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_age_demographic_trends'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Age demographic trends over time with trend direction analysis';

select * from fact_age_demographic_trends;

-- -------------------------------------------------------------------------
-- Data Quality Trends Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_data_quality_trends
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_data_quality_trends'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Data quality trends over time with quality change analysis';

select * from fact_data_quality_trends;

-- -------------------------------------------------------------------------
-- Registration Anomalies Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_registration_anomalies
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_registration_anomalies'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Registration anomalies detection with statistical analysis';

select * from fact_registration_anomalies;

-- -------------------------------------------------------------------------
-- Quality Anomalies Fact Table (Iceberg Table)
-- -------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE fact_quality_anomalies
  EXTERNAL_VOLUME = 'iceberg_vol'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'fact_quality_anomalies'
  CATALOG_NAMESPACE = 'data_pipeline_db'
  AUTO_REFRESH = TRUE
  COMMENT = 'Data quality anomalies detection with z-score analysis';

select * from fact_quality_anomalies;
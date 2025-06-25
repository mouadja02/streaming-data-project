-- =========================================================================
-- Silver Layer: Iceberg Tables (Processed & Enriched Data)
-- =========================================================================
-- This script creates Iceberg external tables for advanced analytics
-- These tables form the Silver layer of the medallion architecture
-- Deployed automatically via CI/CD pipeline


-- -------------------------------------------------------------------------
-- Transformed Users Data (from Glue Job 1)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.users_transformed_ext (
    id STRING AS (value:id::STRING),
    first_name STRING AS (value:first_name::STRING),
    last_name STRING AS (value:last_name::STRING),
    full_name STRING AS (value:full_name::STRING),
    gender STRING AS (value:gender::STRING),
    email STRING AS (value:email::STRING),
    email_domain STRING AS (value:email_domain::STRING),
    email_provider_type STRING AS (value:email_provider_type::STRING),
    email_valid BOOLEAN AS (value:email_valid::BOOLEAN),
    username STRING AS (value:username::STRING),
    phone STRING AS (value:phone::STRING),
    phone_valid BOOLEAN AS (value:phone_valid::BOOLEAN),
    address STRING AS (value:address::STRING),
    street_address STRING AS (value:street_address::STRING),
    city STRING AS (value:city::STRING),
    country STRING AS (value:country::STRING),
    post_code STRING AS (value:post_code::STRING),
    birth_date DATE AS (value:birth_date::DATE),
    age_years NUMBER AS (value:age_years::NUMBER),
    age_group STRING AS (value:age_group::STRING),
    generation STRING AS (value:generation::STRING),
    registration_date DATE AS (value:registration_date::DATE),
    account_age_days NUMBER AS (value:account_age_days::NUMBER),
    account_tenure_category STRING AS (value:account_tenure_category::STRING),
    data_quality_score NUMBER AS (value:data_quality_score::NUMBER),
    quality_issues ARRAY AS (value:quality_issues::ARRAY),
    picture STRING AS (value:picture::STRING),
    processed_at TIMESTAMP AS (value:processed_at::TIMESTAMP),
    processing_date DATE AS (value:processing_date::DATE),
    data_source STRING AS (value:data_source::STRING),
    glue_job_name STRING AS (value:glue_job_name::STRING)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/users_transformed_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Data Quality Summary (from Glue Job 1)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.data_quality_summary_ext (
    total_records NUMBER AS (value:total_records::NUMBER),
    high_quality_records NUMBER AS (value:high_quality_records::NUMBER),
    medium_quality_records NUMBER AS (value:medium_quality_records::NUMBER),
    low_quality_records NUMBER AS (value:low_quality_records::NUMBER),
    avg_quality_score NUMBER AS (value:avg_quality_score::NUMBER),
    valid_emails NUMBER AS (value:valid_emails::NUMBER),
    valid_phones NUMBER AS (value:valid_phones::NUMBER),
    unique_email_domains NUMBER AS (value:unique_email_domains::NUMBER),
    unique_countries NUMBER AS (value:unique_countries::NUMBER),
    quality_check_timestamp TIMESTAMP AS (value:quality_check_timestamp::TIMESTAMP),
    processing_date DATE AS (value:processing_date::DATE),
    glue_job_name STRING AS (value:glue_job_name::STRING)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/data_quality_summary_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- User Demographics Dimension (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.dim_user_demographics_ext (
    user_id STRING AS (value:user_id::STRING),
    age_group STRING AS (value:age_group::STRING),
    generation STRING AS (value:generation::STRING),
    gender STRING AS (value:gender::STRING),
    country STRING AS (value:country::STRING),
    city STRING AS (value:city::STRING),
    email_provider_type STRING AS (value:email_provider_type::STRING),
    account_tenure_category STRING AS (value:account_tenure_category::STRING),
    data_quality_score NUMBER AS (value:data_quality_score::NUMBER),
    processing_date DATE AS (value:processing_date::DATE),
    demographic_score NUMBER AS (value:demographic_score::NUMBER),
    target_segment STRING AS (value:target_segment::STRING),
    dim_created_at TIMESTAMP AS (value:dim_created_at::TIMESTAMP),
    dim_updated_at TIMESTAMP AS (value:dim_updated_at::TIMESTAMP),
    is_current BOOLEAN AS (value:is_current::BOOLEAN)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/dim_user_demographics_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Geographic Analysis Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_geographic_analysis_ext (
    geographic_level STRING AS (value:geographic_level::STRING),
    analysis_type STRING AS (value:analysis_type::STRING),
    user_count NUMBER AS (value:user_count::NUMBER),
    unique_email_domains NUMBER AS (value:unique_email_domains::NUMBER),
    avg_age NUMBER AS (value:avg_age::NUMBER),
    avg_data_quality NUMBER AS (value:avg_data_quality::NUMBER),
    male_percentage NUMBER AS (value:male_percentage::NUMBER),
    female_percentage NUMBER AS (value:female_percentage::NUMBER),
    business_email_percentage NUMBER AS (value:business_email_percentage::NUMBER),
    processing_date DATE AS (value:processing_date::DATE),
    user_count_rank NUMBER AS (value:user_count_rank::NUMBER),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_geographic_analysis_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Age/Generation Analysis Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_age_generation_analysis_ext (
    age_group STRING AS (value:age_group::STRING),
    generation STRING AS (value:generation::STRING),
    gender STRING AS (value:gender::STRING),
    processing_date DATE AS (value:processing_date::DATE),
    user_count NUMBER AS (value:user_count::NUMBER),
    avg_data_quality NUMBER AS (value:avg_data_quality::NUMBER),
    avg_account_age_days NUMBER AS (value:avg_account_age_days::NUMBER),
    countries_represented NUMBER AS (value:countries_represented::NUMBER),
    unique_email_domains NUMBER AS (value:unique_email_domains::NUMBER),
    personal_email_count NUMBER AS (value:personal_email_count::NUMBER),
    business_email_count NUMBER AS (value:business_email_count::NUMBER),
    veteran_users NUMBER AS (value:veteran_users::NUMBER),
    new_users NUMBER AS (value:new_users::NUMBER),
    total_users_in_batch NUMBER AS (value:total_users_in_batch::NUMBER),
    percentage_of_total NUMBER AS (value:percentage_of_total::NUMBER),
    business_email_percentage NUMBER AS (value:business_email_percentage::NUMBER),
    veteran_percentage NUMBER AS (value:veteran_percentage::NUMBER),
    engagement_score NUMBER AS (value:engagement_score::NUMBER),
    engagement_tier STRING AS (value:engagement_tier::STRING),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_age_generation_analysis_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Email Provider Analysis Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_email_provider_analysis_ext (
    email_provider_type STRING AS (value:email_provider_type::STRING),
    processing_date DATE AS (value:processing_date::DATE),
    total_users NUMBER AS (value:total_users::NUMBER),
    unique_domains NUMBER AS (value:unique_domains::NUMBER),
    avg_age NUMBER AS (value:avg_age::NUMBER),
    avg_data_quality NUMBER AS (value:avg_data_quality::NUMBER),
    countries_represented NUMBER AS (value:countries_represented::NUMBER),
    analysis_level STRING AS (value:analysis_level::STRING),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_email_provider_analysis_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Email Domain Analysis Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_email_domain_analysis_ext (
    email_domain STRING AS (value:email_domain::STRING),
    email_provider_type STRING AS (value:email_provider_type::STRING),
    processing_date DATE AS (value:processing_date::DATE),
    user_count NUMBER AS (value:user_count::NUMBER),
    countries_using NUMBER AS (value:countries_using::NUMBER),
    avg_user_age NUMBER AS (value:avg_user_age::NUMBER),
    avg_data_quality NUMBER AS (value:avg_data_quality::NUMBER),
    male_users NUMBER AS (value:male_users::NUMBER),
    female_users NUMBER AS (value:female_users::NUMBER),
    total_users_in_batch NUMBER AS (value:total_users_in_batch::NUMBER),
    market_share_percentage NUMBER AS (value:market_share_percentage::NUMBER),
    gender_ratio_male_female NUMBER AS (value:gender_ratio_male_female::NUMBER),
    analysis_level STRING AS (value:analysis_level::STRING),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_email_domain_analysis_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Data Quality Metrics Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_data_quality_metrics_ext (
    total_records NUMBER AS (value:total_records::NUMBER),
    excellent_quality NUMBER AS (value:excellent_quality::NUMBER),
    good_quality NUMBER AS (value:good_quality::NUMBER),
    fair_quality NUMBER AS (value:fair_quality::NUMBER),
    poor_quality NUMBER AS (value:poor_quality::NUMBER),
    very_poor_quality NUMBER AS (value:very_poor_quality::NUMBER),
    avg_quality_score NUMBER AS (value:avg_quality_score::NUMBER),
    min_quality_score NUMBER AS (value:min_quality_score::NUMBER),
    max_quality_score NUMBER AS (value:max_quality_score::NUMBER),
    valid_emails NUMBER AS (value:valid_emails::NUMBER),
    valid_phones NUMBER AS (value:valid_phones::NUMBER),
    unique_users NUMBER AS (value:unique_users::NUMBER),
    records_with_issues NUMBER AS (value:records_with_issues::NUMBER),
    processing_date DATE AS (value:processing_date::DATE),
    excellent_quality_percentage NUMBER AS (value:excellent_quality_percentage::NUMBER),
    good_quality_percentage NUMBER AS (value:good_quality_percentage::NUMBER),
    email_validity_percentage NUMBER AS (value:email_validity_percentage::NUMBER),
    phone_validity_percentage NUMBER AS (value:phone_validity_percentage::NUMBER),
    data_completeness_score NUMBER AS (value:data_completeness_score::NUMBER),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_data_quality_metrics_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE;

-- -------------------------------------------------------------------------
-- Quality by Segment Fact Table (from Glue Job 2)
-- -------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.fact_quality_by_segment_ext (
    age_group STRING AS (value:age_group::STRING),
    generation STRING AS (value:generation::STRING),
    processing_date DATE AS (value:processing_date::DATE),
    segment_total NUMBER AS (value:segment_total::NUMBER),
    avg_segment_quality NUMBER AS (value:avg_segment_quality::NUMBER),
    valid_emails_in_segment NUMBER AS (value:valid_emails_in_segment::NUMBER),
    valid_phones_in_segment NUMBER AS (value:valid_phones_in_segment::NUMBER),
    segment_email_validity_rate NUMBER AS (value:segment_email_validity_rate::NUMBER),
    segment_phone_validity_rate NUMBER AS (value:segment_phone_validity_rate::NUMBER),
    created_at TIMESTAMP AS (value:created_at::TIMESTAMP)
)
LOCATION = @${SNOWFLAKE_DATABASE}.${SILVER_LAYER}.iceberg_stage/fact_quality_by_segment_parquet/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = TRUE; 
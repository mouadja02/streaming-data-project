-- =========================================================================
-- GOLD LAYER VIEWS CREATION
-- =========================================================================
-- This script creates all analytics views in the GOLD_LAYER schema
-- These views provide pre-computed analytics insights from Bronze and Silver layers

-- Create Gold Layer schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.GOLD_LAYER;



-- ========== TRANSFORMED USERS ANALYSIS ==========

-- Age group and generation distribution
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_AGE_GENERATION_DISTRIBUTION AS
SELECT 
    age_group,
    generation,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED
GROUP BY age_group, generation
ORDER BY age_group;

-- Data quality distribution
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_DISTRIBUTION AS
SELECT 
    CASE 
        WHEN data_quality_score >= 80 THEN 'High Quality'
        WHEN data_quality_score >= 50 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as quality_tier,
    COUNT(*) as count,
    AVG(data_quality_score) as avg_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED
GROUP BY quality_tier
ORDER BY avg_score DESC;

-- Email provider analysis
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_ANALYSIS AS
SELECT 
    email_provider_type,
    COUNT(*) as user_count,
    COUNT(DISTINCT email_domain) as unique_domains
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED
GROUP BY email_provider_type
ORDER BY user_count DESC;


-- ========== DEMOGRAPHIC INSIGHTS ==========

-- Target segment distribution
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TARGET_SEGMENT_DISTRIBUTION AS
SELECT 
    target_segment,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    AVG(data_quality_score) as avg_quality_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.DIM_USER_DEMOGRAPHICS
GROUP BY target_segment
ORDER BY user_count DESC;

-- Demographics by generation and age group
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DEMOGRAPHICS_BY_GENERATION_AGE AS
SELECT 
    generation,
    age_group,
    COUNT(*) as users,
    COUNT(DISTINCT country) as countries,
    AVG(demographic_score) as avg_demographic_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.DIM_USER_DEMOGRAPHICS
GROUP BY generation, age_group
ORDER BY generation, age_group;

-- ========== GEOGRAPHIC ANALYSIS ==========

-- Top countries by user count
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_COUNTRIES AS
SELECT 
    geographic_level as country,
    user_count,
    avg_age,
    male_percentage,
    female_percentage,
    business_email_percentage,
    user_count_rank
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_ANALYSIS
WHERE analysis_type = 'country'
ORDER BY user_count DESC;

-- Geographic diversity analysis
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_DIVERSITY AS
SELECT 
    analysis_type,
    COUNT(*) as total_locations,
    SUM(user_count) as total_users,
    AVG(avg_age) as global_avg_age,
    AVG(business_email_percentage) as avg_business_email_pct
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_ANALYSIS
GROUP BY analysis_type;

-- Cities with highest business email concentration
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_CITIES_BUSINESS_EMAIL_CONCENTRATION AS
SELECT 
    geographic_level,
    user_count,
    business_email_percentage,
    avg_data_quality
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_ANALYSIS
WHERE analysis_type = 'city' 
  AND user_count >= 5  -- Filter for cities with meaningful sample size
ORDER BY business_email_percentage DESC;

-- ========== AGE & GENERATION ANALYSIS ==========

-- Engagement analysis by generation
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_ENGAGEMENT_BY_GENERATION AS
SELECT 
    generation,
    engagement_tier,
    COUNT(*) as segment_count,
    AVG(avg_account_age_days) as avg_account_age,
    AVG(business_email_percentage) as avg_business_email_pct,
    AVG(veteran_percentage) as avg_veteran_pct
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_GENERATION_ANALYSIS
GROUP BY generation, engagement_tier
ORDER BY generation, engagement_tier;

-- Gender distribution across age groups
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GENDER_BY_AGE_GROUP AS
SELECT 
    age_group,
    gender,
    SUM(user_count) as total_users,
    AVG(avg_data_quality) as avg_quality,
    AVG(countries_represented) as avg_countries_represented
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_GENERATION_ANALYSIS
GROUP BY age_group, gender
ORDER BY age_group, gender;

-- High engagement segments
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_HIGH_ENGAGEMENT_SEGMENTS AS
SELECT 
    age_group,
    generation,
    gender,
    user_count,
    engagement_score,
    business_email_percentage,
    veteran_percentage
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_GENERATION_ANALYSIS
WHERE engagement_tier = 'High'
ORDER BY engagement_score DESC;

-- ========== EMAIL PROVIDER ANALYSIS ==========

-- Email provider market share
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE AS
SELECT 
    email_provider_type,
    total_users,
    unique_domains,
    ROUND(total_users * 100.0 / SUM(total_users) OVER(), 2) as market_share_pct,
    avg_age,
    countries_represented
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_EMAIL_PROVIDER_ANALYSIS
ORDER BY total_users DESC;

-- Top email domains analysis
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_EMAIL_DOMAINS AS
SELECT 
    email_domain,
    email_provider_type,
    user_count,
    market_share_percentage,
    avg_user_age,
    countries_using,
    gender_ratio_male_female
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_EMAIL_DOMAIN_ANALYSIS
WHERE user_count >= 2  -- Filter for domains with multiple users
ORDER BY market_share_percentage DESC;

-- Email provider age demographics
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_AGE_DEMOGRAPHICS AS
SELECT 
    ed.email_provider_type,
    ROUND(AVG(ed.avg_user_age), 1) as avg_age,
    COUNT(DISTINCT ed.email_domain) as unique_domains,
    SUM(ed.user_count) as total_users,
    ROUND(AVG(ed.gender_ratio_male_female), 2) as avg_gender_ratio
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_EMAIL_DOMAIN_ANALYSIS ed
GROUP BY ed.email_provider_type
ORDER BY total_users DESC;

-- ========== DATA QUALITY ANALYSIS ==========

-- Overall data quality metrics
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_METRICS AS
SELECT 
    processing_date,
    total_records,
    excellent_quality_percentage,
    good_quality_percentage,
    email_validity_percentage,
    phone_validity_percentage,
    data_completeness_score,
    avg_quality_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_METRICS
ORDER BY processing_date DESC;

-- Quality trends by segment
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUALITY_BY_SEGMENT AS
SELECT 
    qs.age_group,
    qs.generation,
    qs.segment_total,
    qs.avg_segment_quality,
    qs.segment_email_validity_rate,
    qs.segment_phone_validity_rate,
    -- Join with demographics for additional context
    d.target_segment
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_QUALITY_BY_SEGMENT qs
LEFT JOIN (
    SELECT DISTINCT age_group, generation, target_segment 
    FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.DIM_USER_DEMOGRAPHICS
) d ON qs.age_group = d.age_group AND qs.generation = d.generation
ORDER BY qs.avg_segment_quality DESC;

-- Data quality batch distribution
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_BATCH_DISTRIBUTION AS
SELECT 
    CASE 
        WHEN dq.excellent_quality_percentage >= 50 THEN 'Excellent Batch'
        WHEN dq.good_quality_percentage + dq.excellent_quality_percentage >= 70 THEN 'Good Batch'
        ELSE 'Needs Improvement'
    END as batch_quality_tier,
    COUNT(*) as batch_count,
    AVG(dq.avg_quality_score) as avg_score,
    AVG(dq.data_completeness_score) as avg_completeness
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_METRICS dq
GROUP BY batch_quality_tier;

-- -------------------------------------------------------------------------
-- CROSS-LAYER ANALYSIS VIEWS (Bronze + Silver)
-- -------------------------------------------------------------------------

-- Compare Bronze vs Silver data completeness
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_BRONZE_VS_SILVER_COMPLETENESS AS
SELECT 
    'Bronze Layer' as layer,
    COUNT(*) as record_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as emails_present,
    COUNT(CASE WHEN phone IS NOT NULL THEN 1 END) as phones_present,
    NULL as emails_valid,
    NULL as phones_valid
FROM ${SNOWFLAKE_DATABASE}.BRONZE_LAYER.ext_raw_users

UNION ALL

SELECT 
    'Silver Layer' as layer,
    COUNT(*) as record_count,
    NULL as emails_present,
    NULL as phones_present,
    SUM(CASE WHEN email_valid THEN 1 ELSE 0 END) as emails_valid,
    SUM(CASE WHEN phone_valid THEN 1 ELSE 0 END) as phones_valid
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED;

-- Data enrichment impact
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_ENRICHMENT_IMPACT AS
SELECT 
    'Raw Data Fields' as data_type,
    COUNT(*) as record_count,
    'Basic user info only' as enrichment_level
FROM ${SNOWFLAKE_DATABASE}.BRONZE_LAYER.ext_raw_users

UNION ALL

SELECT 
    'Enriched Data Fields' as data_type,
    COUNT(*) as record_count,
    CONCAT(
        'Added: age_group, generation, email_provider_type, ',
        'data_quality_score, account_tenure_category'
    ) as enrichment_level
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED;

-- -------------------------------------------------------------------------
-- SUMMARY ANALYTICS VIEWS
-- -------------------------------------------------------------------------

-- Executive summary view combining key metrics
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY AS
SELECT 
    (SELECT total_users FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_BRONZE_USER_OVERVIEW) as total_users,
    (SELECT COUNT(DISTINCT target_segment) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TARGET_SEGMENT_DISTRIBUTION) as target_segments,
    (SELECT COUNT(DISTINCT analysis_type) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_DIVERSITY) as geographic_levels,
    (SELECT AVG(avg_quality_score) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_DISTRIBUTION) as overall_quality_score,
    (SELECT COUNT(*) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE) as email_provider_types,
    CURRENT_TIMESTAMP() as report_generated_at; 
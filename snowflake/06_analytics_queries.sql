-- =========================================================================
-- Analytics Queries
-- =========================================================================
-- Run these queries after CI/CD has created all the external tables
-- These queries provide insights across Bronze and Silver layers

-- -------------------------------------------------------------------------
-- BRONZE LAYER QUERIES (Basic Data Exploration)
-- -------------------------------------------------------------------------

-- Basic user data overview from Bronze layer
SELECT 
    COUNT(*) as total_users,
    COUNT(DISTINCT gender) as gender_types,
    MIN(dob) as oldest_dob,
    MAX(dob) as youngest_dob
FROM ${SNOWFLAKE_DATABASE}.BRONZE_LAYER.ext_raw_users;

-- Geographic distribution from Bronze layer
SELECT 
    country,
    user_count,
    avg_age,
    male_percentage,
    female_percentage
FROM ${SNOWFLAKE_DATABASE}.BRONZE_LAYER.ext_user_demographics
ORDER BY user_count DESC
LIMIT 10;

-- -------------------------------------------------------------------------
-- SILVER LAYER QUERIES (Advanced Analytics)
-- -------------------------------------------------------------------------

-- ========== TRANSFORMED USERS ANALYSIS ==========

-- Sample transformed users data
SELECT * FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext LIMIT 10;

-- Age group and generation distribution
SELECT 
    age_group,
    generation,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext
GROUP BY age_group, generation
ORDER BY age_group;

-- Data quality distribution
SELECT 
    CASE 
        WHEN data_quality_score >= 80 THEN 'High Quality'
        WHEN data_quality_score >= 50 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as quality_tier,
    COUNT(*) as count,
    AVG(data_quality_score) as avg_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext
GROUP BY quality_tier
ORDER BY avg_score DESC;

-- Email provider analysis
SELECT 
    email_provider_type,
    COUNT(*) as user_count,
    COUNT(DISTINCT email_domain) as unique_domains
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext
GROUP BY email_provider_type
ORDER BY user_count DESC;

-- ========== DATA QUALITY SUMMARY ==========

-- Overall data quality summary
SELECT * FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.data_quality_summary_ext;

-- ========== DEMOGRAPHIC INSIGHTS ==========

-- Target segment distribution
SELECT 
    target_segment,
    COUNT(*) as user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    AVG(data_quality_score) as avg_quality_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.dim_user_demographics_ext
GROUP BY target_segment
ORDER BY user_count DESC;

-- Demographics by generation and age group
SELECT 
    generation,
    age_group,
    COUNT(*) as users,
    COUNT(DISTINCT country) as countries,
    AVG(demographic_score) as avg_demographic_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.dim_user_demographics_ext
GROUP BY generation, age_group
ORDER BY generation, age_group;

-- ========== GEOGRAPHIC ANALYSIS ==========

-- Top countries by user count
SELECT 
    geographic_level as country,
    user_count,
    avg_age,
    male_percentage,
    female_percentage,
    business_email_percentage,
    user_count_rank
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_geographic_analysis_ext
WHERE analysis_type = 'country'
ORDER BY user_count DESC
LIMIT 10;

-- Geographic diversity analysis
SELECT 
    analysis_type,
    COUNT(*) as total_locations,
    SUM(user_count) as total_users,
    AVG(avg_age) as global_avg_age,
    AVG(business_email_percentage) as avg_business_email_pct
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_geographic_analysis_ext
GROUP BY analysis_type;

-- Cities with highest business email concentration
SELECT 
    geographic_level,
    user_count,
    business_email_percentage,
    avg_data_quality
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_geographic_analysis_ext
WHERE analysis_type = 'city' 
  AND user_count >= 5  -- Filter for cities with meaningful sample size
ORDER BY business_email_percentage DESC
LIMIT 15;

-- ========== AGE & GENERATION ANALYSIS ==========

-- Engagement analysis by generation
SELECT 
    generation,
    engagement_tier,
    COUNT(*) as segment_count,
    AVG(avg_account_age_days) as avg_account_age,
    AVG(business_email_percentage) as avg_business_email_pct,
    AVG(veteran_percentage) as avg_veteran_pct
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_age_generation_analysis_ext
GROUP BY generation, engagement_tier
ORDER BY generation, engagement_tier;

-- Gender distribution across age groups
SELECT 
    age_group,
    gender,
    SUM(user_count) as total_users,
    AVG(avg_data_quality) as avg_quality,
    AVG(countries_represented) as avg_countries_represented
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_age_generation_analysis_ext
GROUP BY age_group, gender
ORDER BY age_group, gender;

-- High engagement segments
SELECT 
    age_group,
    generation,
    gender,
    user_count,
    engagement_score,
    business_email_percentage,
    veteran_percentage
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_age_generation_analysis_ext
WHERE engagement_tier = 'High'
ORDER BY engagement_score DESC;

-- ========== EMAIL PROVIDER ANALYSIS ==========

-- Email provider market share
SELECT 
    email_provider_type,
    total_users,
    unique_domains,
    ROUND(total_users * 100.0 / SUM(total_users) OVER(), 2) as market_share_pct,
    avg_age,
    countries_represented
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_email_provider_analysis_ext
ORDER BY total_users DESC;

-- Top email domains analysis
SELECT 
    email_domain,
    email_provider_type,
    user_count,
    market_share_percentage,
    avg_user_age,
    countries_using,
    gender_ratio_male_female
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_email_domain_analysis_ext
WHERE user_count >= 2  -- Filter for domains with multiple users
ORDER BY market_share_percentage DESC
LIMIT 20;

-- Email provider age demographics
SELECT 
    ed.email_provider_type,
    ROUND(AVG(ed.avg_user_age), 1) as avg_age,
    COUNT(DISTINCT ed.email_domain) as unique_domains,
    SUM(ed.user_count) as total_users,
    ROUND(AVG(ed.gender_ratio_male_female), 2) as avg_gender_ratio
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_email_domain_analysis_ext ed
GROUP BY ed.email_provider_type
ORDER BY total_users DESC;

-- ========== DATA QUALITY ANALYSIS ==========

-- Overall data quality summary
SELECT 
    processing_date,
    total_records,
    excellent_quality_percentage,
    good_quality_percentage,
    email_validity_percentage,
    phone_validity_percentage,
    data_completeness_score,
    avg_quality_score
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_data_quality_metrics_ext
ORDER BY processing_date DESC;

-- Quality trends by segment
SELECT 
    qs.age_group,
    qs.generation,
    qs.segment_total,
    qs.avg_segment_quality,
    qs.segment_email_validity_rate,
    qs.segment_phone_validity_rate,
    -- Join with demographics for additional context
    d.target_segment
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_quality_by_segment_ext qs
LEFT JOIN (
    SELECT DISTINCT age_group, generation, target_segment 
    FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.dim_user_demographics_ext
) d ON qs.age_group = d.age_group AND qs.generation = d.generation
ORDER BY qs.avg_segment_quality DESC;

-- Data quality distribution
SELECT 
    CASE 
        WHEN dq.excellent_quality_percentage >= 50 THEN 'Excellent Batch'
        WHEN dq.good_quality_percentage + dq.excellent_quality_percentage >= 70 THEN 'Good Batch'
        ELSE 'Needs Improvement'
    END as batch_quality_tier,
    COUNT(*) as batch_count,
    AVG(dq.avg_quality_score) as avg_score,
    AVG(dq.data_completeness_score) as avg_completeness
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.fact_data_quality_metrics_ext dq
GROUP BY batch_quality_tier;

-- -------------------------------------------------------------------------
-- CROSS-LAYER ANALYSIS (Bronze + Silver)
-- -------------------------------------------------------------------------

-- Compare Bronze vs Silver data completeness
SELECT 
    'Bronze Layer' as layer,
    COUNT(*) as record_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as emails_present,
    COUNT(CASE WHEN phone IS NOT NULL THEN 1 END) as phones_present
FROM ${SNOWFLAKE_DATABASE}.BRONZE_LAYER.ext_raw_users

UNION ALL

SELECT 
    'Silver Layer' as layer,
    COUNT(*) as record_count,
    SUM(CASE WHEN email_valid THEN 1 ELSE 0 END) as emails_valid,
    SUM(CASE WHEN phone_valid THEN 1 ELSE 0 END) as phones_valid
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext;

-- Data enrichment impact
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
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.users_transformed_ext; 
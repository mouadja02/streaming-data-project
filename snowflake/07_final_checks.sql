-- =========================================================================
-- GOLD LAYER USAGE EXAMPLES
-- =========================================================================
-- This script demonstrates how to use the Gold Layer views for analytics
-- Run this after executing 07_create_gold_views.sql

-- -------------------------------------------------------------------------
-- BASIC VIEW USAGE EXAMPLES
-- -------------------------------------------------------------------------

-- Get overall user overview
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_BRONZE_USER_OVERVIEW;

-- View top 10 countries by user count
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_COUNTRIES LIMIT 10;

-- Check data quality distribution
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_DISTRIBUTION;

-- View email provider market share
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE;

-- -------------------------------------------------------------------------
-- ADVANCED ANALYTICS COMBINATIONS
-- -------------------------------------------------------------------------

-- Combine demographic and quality insights
SELECT 
    demo.target_segment,
    demo.user_count,
    demo.percentage as demo_percentage,
    qual.count as quality_count,
    qual.avg_score as quality_score
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TARGET_SEGMENT_DISTRIBUTION demo
LEFT JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_DISTRIBUTION qual
    ON demo.avg_quality_score BETWEEN 
        CASE qual.quality_tier 
            WHEN 'High Quality' THEN 80 
            WHEN 'Medium Quality' THEN 50 
            ELSE 0 
        END 
        AND 
        CASE qual.quality_tier 
            WHEN 'High Quality' THEN 100 
            WHEN 'Medium Quality' THEN 79 
            ELSE 49 
        END
ORDER BY demo.user_count DESC;

-- Geographic and engagement correlation
SELECT 
    geo.country,
    geo.user_count as country_users,
    geo.business_email_percentage,
    eng.segment_count as high_engagement_segments,
    eng.avg_business_email_pct as engagement_business_email_pct
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_COUNTRIES geo
LEFT JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_ENGAGEMENT_BY_GENERATION eng
    ON eng.engagement_tier = 'High'
WHERE geo.user_count_rank <= 10
ORDER BY geo.user_count DESC;

-- Email provider and age group analysis
SELECT 
    emp.email_provider_type,
    emp.market_share_pct,
    age_demo.avg_age as provider_avg_age,
    age_demo.unique_domains
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE emp
JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_AGE_DEMOGRAPHICS age_demo
    ON emp.email_provider_type = age_demo.email_provider_type
ORDER BY emp.market_share_pct DESC;

-- -------------------------------------------------------------------------
-- BUSINESS INTELLIGENCE QUERIES
-- -------------------------------------------------------------------------

-- Executive dashboard summary
SELECT 
    'Total Users' as metric,
    CAST(total_users AS VARCHAR) as value,
    'users' as unit
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY

UNION ALL

SELECT 
    'Target Segments' as metric,
    CAST(target_segments AS VARCHAR) as value,
    'segments' as unit
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY

UNION ALL

SELECT 
    'Geographic Levels' as metric,
    CAST(geographic_levels AS VARCHAR) as value,
    'levels' as unit
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY

UNION ALL

SELECT 
    'Overall Quality Score' as metric,
    CAST(ROUND(overall_quality_score, 2) AS VARCHAR) as value,
    'score' as unit
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY;

-- Data quality health check
SELECT 
    'Excellent Quality' as quality_tier,
    COUNT(*) as batch_count,
    ROUND(AVG(avg_score), 2) as avg_score
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_BATCH_DISTRIBUTION
WHERE batch_quality_tier = 'Excellent Batch'

UNION ALL

SELECT 
    'Good Quality' as quality_tier,
    COUNT(*) as batch_count,
    ROUND(AVG(avg_score), 2) as avg_score
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_BATCH_DISTRIBUTION
WHERE batch_quality_tier = 'Good Batch'

UNION ALL

SELECT 
    'Needs Improvement' as quality_tier,
    COUNT(*) as batch_count,
    ROUND(AVG(avg_score), 2) as avg_score
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_BATCH_DISTRIBUTION
WHERE batch_quality_tier = 'Needs Improvement';

-- Geographic business opportunity analysis
SELECT 
    geo.country,
    geo.user_count,
    geo.business_email_percentage,
    cities.geographic_level as top_city,
    cities.business_email_percentage as city_business_pct
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_COUNTRIES geo
LEFT JOIN (
    SELECT 
        geographic_level,
        business_email_percentage,
        ROW_NUMBER() OVER (ORDER BY business_email_percentage DESC) as rn
    FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_CITIES_BUSINESS_EMAIL_CONCENTRATION
) cities ON cities.rn = 1
WHERE geo.user_count_rank <= 5
ORDER BY geo.business_email_percentage DESC;

-- Age and engagement insights
SELECT 
    age.age_group,
    age.total_users,
    eng.segment_count as high_engagement_count,
    ROUND((eng.segment_count * 100.0 / age.total_users), 2) as engagement_rate_pct,
    eng.avg_business_email_pct
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GENDER_BY_AGE_GROUP age
LEFT JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_ENGAGEMENT_BY_GENERATION eng
    ON eng.engagement_tier = 'High'
GROUP BY age.age_group, age.total_users, eng.segment_count, eng.avg_business_email_pct
ORDER BY engagement_rate_pct DESC NULLS LAST;

-- -------------------------------------------------------------------------
-- MONITORING AND ALERTING QUERIES
-- -------------------------------------------------------------------------

-- Data freshness check
SELECT 
    'Gold Layer Views' as layer,
    COUNT(*) as view_count,
    report_generated_at as last_update
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EXECUTIVE_SUMMARY
GROUP BY report_generated_at;

-- Quality alert check (identify potential data issues)
SELECT 
    'LOW_QUALITY_ALERT' as alert_type,
    batch_quality_tier,
    batch_count,
    avg_score,
    CASE 
        WHEN avg_score < 30 THEN 'CRITICAL'
        WHEN avg_score < 50 THEN 'WARNING'
        ELSE 'OK'
    END as severity
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_BATCH_DISTRIBUTION
WHERE batch_quality_tier = 'Needs Improvement'
   OR avg_score < 50;

-- Data completeness comparison alert
SELECT 
    'COMPLETENESS_COMPARISON' as alert_type,
    layer,
    record_count,
    emails_present,
    phones_present,
    emails_valid,
    phones_valid,
    CASE 
        WHEN layer = 'Silver Layer' AND emails_valid < (emails_present * 0.8) 
        THEN 'EMAIL_VALIDATION_LOW'
        WHEN layer = 'Silver Layer' AND phones_valid < (phones_present * 0.8) 
        THEN 'PHONE_VALIDATION_LOW'
        ELSE 'OK'
    END as alert_status
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_BRONZE_VS_SILVER_COMPLETENESS;

-- -------------------------------------------------------------------------
-- VIEW MANAGEMENT QUERIES
-- -------------------------------------------------------------------------

-- List all Gold Layer views
SHOW VIEWS IN SCHEMA ${SNOWFLAKE_DATABASE}.GOLD_LAYER;

-- Get view definitions (useful for documentation)
-- Note: Replace 'VW_BRONZE_USER_OVERVIEW' with any specific view name
-- SHOW CREATE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_BRONZE_USER_OVERVIEW;

-- Check view dependencies
-- SELECT * FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE 
-- WHERE VIEW_SCHEMA = 'GOLD_LAYER'; 
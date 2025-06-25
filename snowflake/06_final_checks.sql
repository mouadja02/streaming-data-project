-- =========================================================================
-- GOLD LAYER USAGE EXAMPLES
-- =========================================================================
-- This script demonstrates how to use the Gold Layer views for analytics
-- Run this after executing 07_create_gold_views.sql

-- -------------------------------------------------------------------------
-- BASIC VIEW USAGE EXAMPLES
-- -------------------------------------------------------------------------

-- Get overall comparaison between bronze and silver layers
SELECT * FROM ECOMMERCE_DB.GOLD_LAYER.VW_BRONZE_VS_SILVER_COMPLETENESS;

-- View top 10 countries by user count
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_COUNTRIES LIMIT 10;

-- Check data quality distribution
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_DISTRIBUTION;

-- View email provider market share
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE;

-- -------------------------------------------------------------------------
-- TIME SERIES ANALYSIS VIEWS EXAMPLES
-- -------------------------------------------------------------------------

-- Check monthly registration trends
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_REGISTRATION_TRENDS LIMIT 12;

-- View quarterly performance
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUARTERLY_REGISTRATION_PERFORMANCE;

-- Check day of week patterns
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_REGISTRATION_DAY_PATTERNS;

-- View top growing countries
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_GROWING_COUNTRIES LIMIT 10;

-- Check data quality trends
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_TREND_OVERVIEW LIMIT 10;

-- View anomaly alerts
SELECT * FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_ANOMALY_ALERT_DASHBOARD;

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

-- Time series executive dashboard insights
SELECT 
    ts.metric_category,
    ts.metric_type,
    ts.period,
    ts.value,
    ts.growth_rate,
    ts.processing_date
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TIME_SERIES_EXECUTIVE_DASHBOARD ts
ORDER BY ts.processing_date DESC, ts.growth_rate DESC
LIMIT 20;

-- Geographic expansion insights by category
SELECT 
    expansion_category,
    countries_in_category,
    total_users,
    avg_acquisition_rate,
    avg_market_share,
    earliest_entry_date,
    latest_entry_date
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_EXPANSION_INSIGHTS
ORDER BY total_users DESC;

-- Age demographic trends analysis
SELECT 
    age_group,
    generation,
    registration_year,
    trend_direction,
    trend_intensity,
    trend_magnitude,
    total_users_in_group
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_AGE_GROUP_TRENDS
WHERE trend_direction IN ('Growing', 'Rapidly Growing')
ORDER BY trend_magnitude DESC
LIMIT 15;

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

-- Monthly business performance summary
SELECT 
    month,
    new_registrations,
    registration_growth_rate,
    countries_active,
    avg_new_user_age,
    data_quality_score,
    quality_direction,
    overall_performance
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_BUSINESS_SUMMARY
ORDER BY month DESC
LIMIT 12;

-- Country expansion timeline analysis
SELECT 
    country,
    first_user_date,
    expansion_year,
    expansion_month,
    user_count,
    expansion_category,
    country_rank_in_year
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_COUNTRY_EXPANSION_TIMELINE
ORDER BY first_user_date DESC
LIMIT 20;

-- Generation performance analysis
SELECT 
    generation,
    registration_year,
    total_generation_users,
    avg_percentage_of_year,
    age_groups_represented,
    avg_data_quality,
    total_countries_reached
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GENERATION_PERFORMANCE_TIMELINE
ORDER BY registration_year DESC, total_generation_users DESC
LIMIT 15;

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
-- TIME SERIES ANOMALY MONITORING
-- -------------------------------------------------------------------------

-- Registration anomalies alert check
SELECT 
    'REGISTRATION_ANOMALY' as alert_type,
    period_type,
    time_period,
    registrations,
    z_score,
    anomaly_severity,
    action_required,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_REGISTRATION_ANOMALIES_SUMMARY
WHERE anomaly_severity IN ('Critical', 'High')
ORDER BY z_score DESC, processing_date DESC;

-- Quality anomalies alert check
SELECT 
    'QUALITY_ANOMALY' as alert_type,
    processing_date,
    avg_quality_score,
    z_score,
    anomaly_severity,
    quality_status,
    total_records
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUALITY_ANOMALIES_SUMMARY
WHERE anomaly_severity IN ('Critical', 'High')
ORDER BY z_score DESC, processing_date DESC;

-- Quality improvement tracking alert
SELECT 
    'QUALITY_TREND_ALERT' as alert_type,
    processing_date,
    quality_trend,
    change_direction,
    change_magnitude,
    high_quality_percentage,
    CASE 
        WHEN change_direction = 'Negative' AND change_magnitude > 5 THEN 'CRITICAL_DECLINE'
        WHEN change_direction = 'Negative' AND change_magnitude > 2 THEN 'MODERATE_DECLINE'
        WHEN change_direction = 'Positive' AND change_magnitude > 5 THEN 'SIGNIFICANT_IMPROVEMENT'
        ELSE 'NORMAL_VARIATION'
    END as trend_alert_level
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUALITY_IMPROVEMENT_TRACKING
WHERE change_direction != 'Neutral'
ORDER BY processing_date DESC, change_magnitude DESC
LIMIT 10;

-- Growth rate monitoring
SELECT 
    'GROWTH_RATE_ALERT' as alert_type,
    time_period,
    registrations,
    growth_rate,
    growth_category,
    CASE 
        WHEN growth_rate < -20 THEN 'CRITICAL_DECLINE'
        WHEN growth_rate < -10 THEN 'MODERATE_DECLINE'
        WHEN growth_rate > 50 THEN 'EXCEPTIONAL_GROWTH'
        WHEN growth_rate > 20 THEN 'HIGH_GROWTH'
        ELSE 'NORMAL_GROWTH'
    END as growth_alert_level
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_REGISTRATION_TRENDS
WHERE ABS(growth_rate) > 10 OR growth_rate IS NULL
ORDER BY processing_date DESC, ABS(growth_rate) DESC
LIMIT 15;

-- -------------------------------------------------------------------------
-- ADVANCED TIME SERIES ANALYTICS
-- -------------------------------------------------------------------------

-- Cross-analysis: Registration trends vs Quality trends
SELECT 
    rt.time_period as month,
    rt.registrations,
    rt.growth_rate as registration_growth,
    qt.avg_quality_score,
    qt.quality_change,
    qt.quality_trend,
    CASE 
        WHEN rt.growth_rate > 0 AND qt.quality_change > 0 THEN 'Growth with Quality Improvement'
        WHEN rt.growth_rate > 0 AND qt.quality_change < 0 THEN 'Growth with Quality Decline'
        WHEN rt.growth_rate < 0 AND qt.quality_change > 0 THEN 'Decline with Quality Improvement'
        WHEN rt.growth_rate < 0 AND qt.quality_change < 0 THEN 'Decline with Quality Decline'
        ELSE 'Mixed or Stable'
    END as combined_trend_analysis
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_REGISTRATION_TRENDS rt
LEFT JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_TREND_OVERVIEW qt
    ON rt.processing_date = qt.processing_date
ORDER BY rt.time_period DESC
LIMIT 12;

-- Fastest growing demographics with expansion correlation
SELECT 
    demo.age_group,
    demo.generation,
    demo.trend_magnitude,
    demo.total_users_in_group,
    geo.expansion_category,
    geo.avg_acquisition_rate,
    geo.total_users as geo_total_users
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_FASTEST_GROWING_DEMOGRAPHICS demo
LEFT JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_EXPANSION_INSIGHTS geo
    ON demo.processing_date = geo.processing_date
WHERE demo.growth_rank <= 5
ORDER BY demo.trend_magnitude DESC;

-- Market penetration vs registration patterns
SELECT 
    geo.expansion_category,
    geo.countries_in_category,
    geo.total_users,
    patterns.day_name,
    patterns.registrations as day_registrations,
    patterns.percentage_of_weekly_registrations
FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MARKET_PENETRATION_ANALYSIS geo
CROSS JOIN ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_REGISTRATION_DAY_PATTERNS patterns
WHERE patterns.popularity_rank <= 3
ORDER BY geo.total_users DESC, patterns.percentage_of_weekly_registrations DESC;

-- -------------------------------------------------------------------------
-- COMPREHENSIVE HEALTH CHECK DASHBOARD
-- -------------------------------------------------------------------------

-- Overall pipeline health summary
SELECT 
    'Pipeline Health Check' as report_type,
    COUNT(DISTINCT rt.time_period) as months_with_data,
    AVG(rt.registrations) as avg_monthly_registrations,
    AVG(rt.growth_rate) as avg_growth_rate,
    COUNT(DISTINCT ge.country) as countries_active,
    AVG(qt.avg_quality_score) as avg_quality_score,
    COUNT(CASE WHEN ra.is_anomaly THEN 1 END) as registration_anomalies,
    COUNT(CASE WHEN qa.is_anomaly THEN 1 END) as quality_anomalies,
    CURRENT_TIMESTAMP() as report_generated_at
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS rt
LEFT JOIN ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION ge
    ON rt.processing_date = ge.processing_date
LEFT JOIN ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS qt
    ON rt.processing_date = qt.processing_date
LEFT JOIN ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_ANOMALIES ra
    ON rt.processing_date = ra.processing_date AND ra.is_anomaly = TRUE
LEFT JOIN ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_QUALITY_ANOMALIES qa
    ON rt.processing_date = qa.processing_date AND qa.is_anomaly = TRUE
WHERE rt.period_type = 'monthly';

-- Time series data completeness check
SELECT 
    'Time Series Completeness' as check_type,
    'Registration Trends' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT processing_date) as unique_dates,
    MIN(processing_date) as earliest_date,
    MAX(processing_date) as latest_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS

UNION ALL

SELECT 
    'Time Series Completeness',
    'Geographic Expansion',
    COUNT(*),
    COUNT(DISTINCT processing_date),
    MIN(processing_date),
    MAX(processing_date)
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION

UNION ALL

SELECT 
    'Time Series Completeness',
    'Age Demographic Trends',
    COUNT(*),
    COUNT(DISTINCT processing_date),
    MIN(processing_date),
    MAX(processing_date)
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_DEMOGRAPHIC_TRENDS

UNION ALL

SELECT 
    'Time Series Completeness',
    'Data Quality Trends',
    COUNT(*),
    COUNT(DISTINCT processing_date),
    MIN(processing_date),
    MAX(processing_date)
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS

ORDER BY table_name;

-- -------------------------------------------------------------------------
-- VIEW MANAGEMENT QUERIES
-- -------------------------------------------------------------------------

-- List all Gold Layer views
SHOW VIEWS IN SCHEMA ${SNOWFLAKE_DATABASE}.GOLD_LAYER;

-- Count of views by category
-- Count of views by category
SELECT 
    CASE 
        WHEN TABLE_NAME LIKE '%REGISTRATION%' THEN 'Registration Analysis'
        WHEN TABLE_NAME LIKE '%GEOGRAPHIC%' OR TABLE_NAME LIKE '%COUNTRY%' THEN 'Geographic Analysis'
        WHEN TABLE_NAME LIKE '%AGE%' OR TABLE_NAME LIKE '%DEMOGRAPHIC%' OR TABLE_NAME LIKE '%GENERATION%' THEN 'Demographic Analysis'
        WHEN TABLE_NAME LIKE '%QUALITY%' THEN 'Quality Analysis'
        WHEN TABLE_NAME LIKE '%ANOMAL%' THEN 'Anomaly Detection'
        WHEN TABLE_NAME LIKE '%EXECUTIVE%' OR TABLE_NAME LIKE '%DASHBOARD%' OR TABLE_NAME LIKE '%SUMMARY%' THEN 'Executive Dashboards'
        WHEN TABLE_NAME LIKE '%EMAIL%' THEN 'Email Analysis'
        ELSE 'Other Analytics'
    END as view_category,
    COUNT(*) as view_count
FROM (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'GOLD_LAYER')
GROUP BY view_category
ORDER BY view_count DESC;


-- Get view definitions (useful for documentation)
-- Note: Replace view name as needed
-- SHOW CREATE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TIME_SERIES_EXECUTIVE_DASHBOARD;

-- Check view dependencies
-- SELECT * FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE 
-- WHERE VIEW_SCHEMA = 'GOLD_LAYER'; 
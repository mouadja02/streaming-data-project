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
    (SELECT COUNT(*) FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.USERS_TRANSFORMED) as total_users,
    (SELECT COUNT(DISTINCT target_segment) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TARGET_SEGMENT_DISTRIBUTION) as target_segments,
    (SELECT COUNT(DISTINCT analysis_type) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_DIVERSITY) as geographic_levels,
    (SELECT ROUND(AVG(avg_quality_score), 2) FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_METRICS) as overall_quality_score,
    (SELECT COUNT(*) FROM ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_EMAIL_PROVIDER_MARKET_SHARE) as email_provider_types,
    CURRENT_TIMESTAMP() as report_generated_at;

-- =========================================================================
-- TIME SERIES ANALYSIS VIEWS (GOLD LAYER)
-- =========================================================================

-- ========== REGISTRATION TRENDS ANALYSIS ==========

-- Monthly registration trends with growth rates
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_REGISTRATION_TRENDS AS
SELECT 
    time_period,
    registrations,
    countries_represented,
    ROUND(avg_age_at_registration, 1) as avg_age_at_registration,
    male_percentage,
    female_percentage,
    ROUND(avg_data_quality, 1) as avg_data_quality,
    growth_rate,
    CASE 
        WHEN growth_rate > 20 THEN 'High Growth'
        WHEN growth_rate > 0 THEN 'Growing'
        WHEN growth_rate < -10 THEN 'Declining'
        ELSE 'Stable'
    END as growth_category,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS
WHERE period_type = 'monthly'
ORDER BY time_period DESC;

-- Quarterly registration performance
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUARTERLY_REGISTRATION_PERFORMANCE AS
SELECT 
    time_period,
    registrations,
    countries_represented,
    ROUND(avg_age_at_registration, 1) as avg_age_at_registration,
    growth_rate,
    RANK() OVER (ORDER BY registrations DESC) as performance_rank,
    CASE 
        WHEN RANK() OVER (ORDER BY registrations DESC) <= 2 THEN 'Top Performer'
        WHEN RANK() OVER (ORDER BY registrations DESC) <= 4 THEN 'Good Performer'
        ELSE 'Needs Improvement'
    END as performance_tier,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS
WHERE period_type = 'quarterly'
ORDER BY time_period DESC;

-- Day of week registration patterns
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_REGISTRATION_DAY_PATTERNS AS
SELECT 
    time_period as day_name,
    registrations,
    ROUND(avg_age_at_registration, 1) as avg_age_at_registration,
    ROUND(avg_data_quality, 1) as avg_data_quality,
    RANK() OVER (ORDER BY registrations DESC) as popularity_rank,
    ROUND(registrations * 100.0 / SUM(registrations) OVER(), 2) as percentage_of_weekly_registrations,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS
WHERE period_type = 'day_of_week'
ORDER BY registrations DESC;

-- ========== GEOGRAPHIC EXPANSION ANALYSIS ==========

-- Top growing countries by expansion rate
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TOP_GROWING_COUNTRIES AS
SELECT 
    country,
    user_count,
    user_acquisition_rate,
    market_share_percentage,
    expansion_category,
    cities_represented,
    ROUND(avg_user_age, 1) as avg_user_age,
    ROUND(avg_data_quality, 1) as avg_data_quality,
    days_since_first_user,
    first_user_date,
    user_count_rank,
    expansion_rate_rank,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION
WHERE expansion_category IN ('Top Tier', 'Mid Tier')
ORDER BY user_acquisition_rate DESC;

-- Market penetration analysis
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MARKET_PENETRATION_ANALYSIS AS
SELECT 
    expansion_category,
    COUNT(*) as country_count,
    SUM(user_count) as total_users,
    ROUND(AVG(market_share_percentage), 2) as avg_market_share,
    ROUND(AVG(user_acquisition_rate), 4) as avg_acquisition_rate,
    ROUND(AVG(cities_represented), 1) as avg_cities_per_country,
    MIN(first_user_date) as earliest_expansion_date,
    MAX(first_user_date) as latest_expansion_date,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION
GROUP BY expansion_category, processing_date
ORDER BY total_users DESC;

-- Country expansion timeline
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_COUNTRY_EXPANSION_TIMELINE AS
SELECT 
    country,
    first_user_date,
    user_count,
    user_acquisition_rate,
    expansion_category,
    EXTRACT(YEAR FROM first_user_date) as expansion_year,
    EXTRACT(MONTH FROM first_user_date) as expansion_month,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM first_user_date) ORDER BY first_user_date) as country_rank_in_year,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION
ORDER BY first_user_date;

-- ========== AGE DEMOGRAPHIC TRENDS ANALYSIS ==========

-- Age group trend analysis
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_AGE_GROUP_TRENDS AS
SELECT 
    age_group,
    generation,
    registration_year,
    total_users_in_group,
    percentage_of_year,
    trend_direction,
    ROUND(trend_magnitude, 2) as trend_magnitude,
    CASE 
        WHEN trend_direction = 'Growing' AND trend_magnitude > 5 THEN 'Rapidly Growing'
        WHEN trend_direction = 'Growing' THEN 'Growing'
        WHEN trend_direction = 'Declining' AND trend_magnitude > 5 THEN 'Rapidly Declining'
        WHEN trend_direction = 'Declining' THEN 'Declining'
        ELSE trend_direction
    END as trend_intensity,
    ROUND(avg_data_quality, 1) as avg_data_quality,
    total_countries,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_DEMOGRAPHIC_TRENDS
ORDER BY registration_year DESC, trend_magnitude DESC;

-- Generation performance over time
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GENERATION_PERFORMANCE_TIMELINE AS
SELECT 
    generation,
    registration_year,
    SUM(total_users_in_group) as total_generation_users,
    ROUND(AVG(percentage_of_year), 2) as avg_percentage_of_year,
    COUNT(DISTINCT age_group) as age_groups_represented,
    ROUND(AVG(avg_data_quality), 1) as avg_data_quality,
    SUM(total_countries) as total_countries_reached,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_DEMOGRAPHIC_TRENDS
GROUP BY generation, registration_year, processing_date
ORDER BY registration_year DESC, total_generation_users DESC;

-- Fastest growing demographic segments
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_FASTEST_GROWING_DEMOGRAPHICS AS
SELECT 
    age_group,
    generation,
    registration_year,
    total_users_in_group,
    trend_magnitude,
    trend_direction,
    percentage_of_year,
    RANK() OVER (PARTITION BY registration_year ORDER BY trend_magnitude DESC) as growth_rank,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_AGE_DEMOGRAPHIC_TRENDS
WHERE trend_direction = 'Growing'
ORDER BY registration_year DESC, trend_magnitude DESC;

-- ========== DATA QUALITY TRENDS ANALYSIS ==========

-- Data quality trend overview
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_DATA_QUALITY_TREND_OVERVIEW AS
SELECT 
    processing_date,
    total_records,
    ROUND(avg_quality_score, 2) as avg_quality_score,
    excellent_quality_percentage,
    good_quality_percentage,
    email_validity_percentage,
    phone_validity_percentage,
    quality_trend,
    ROUND(quality_change, 2) as quality_change,
    CASE 
        WHEN quality_trend = 'Improving' AND quality_change > 2 THEN 'Significantly Improving'
        WHEN quality_trend = 'Declining' AND quality_change < -2 THEN 'Significantly Declining'
        ELSE quality_trend
    END as quality_trend_intensity,
    LAG(avg_quality_score, 1) OVER (ORDER BY processing_date) as previous_avg_quality
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS
ORDER BY processing_date DESC;

-- Quality improvement tracking
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUALITY_IMPROVEMENT_TRACKING AS
SELECT 
    processing_date,
    avg_quality_score,
    quality_change,
    quality_trend,
    CASE 
        WHEN quality_change > 0 THEN 'Positive'
        WHEN quality_change < 0 THEN 'Negative' 
        ELSE 'Neutral'
    END as change_direction,
    ABS(quality_change) as change_magnitude,
    excellent_quality_count + good_quality_count as high_quality_records,
    ROUND((excellent_quality_count + good_quality_count) * 100.0 / total_records, 2) as high_quality_percentage,
    total_records
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS
ORDER BY processing_date DESC;

-- ========== ANOMALY DETECTION ANALYSIS ==========

-- Registration anomalies summary
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_REGISTRATION_ANOMALIES_SUMMARY AS
SELECT 
    period_type,
    time_period,
    registrations,
    ROUND(z_score, 2) as z_score,
    anomaly_type,
    anomaly_severity,
    growth_rate,
    CASE 
        WHEN anomaly_severity = 'Critical' THEN 'Immediate Attention Required'
        WHEN anomaly_severity = 'High' THEN 'Investigation Recommended'
        WHEN anomaly_severity = 'Medium' THEN 'Monitor Closely'
        ELSE 'Normal Variation'
    END as action_required,
    processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_ANOMALIES
WHERE is_anomaly = TRUE
ORDER BY z_score DESC, processing_date DESC;

-- Quality anomalies summary
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_QUALITY_ANOMALIES_SUMMARY AS
SELECT 
    processing_date,
    ROUND(avg_quality_score, 2) as avg_quality_score,
    ROUND(z_score, 2) as z_score,
    anomaly_type,
    anomaly_severity,
    quality_trend,
    ROUND(quality_change, 2) as quality_change,
    CASE 
        WHEN anomaly_severity = 'Critical' THEN 'Data Quality Crisis'
        WHEN anomaly_severity = 'High' THEN 'Quality Issue Detected'
        WHEN anomaly_severity = 'Medium' THEN 'Quality Variation'
        ELSE 'Normal Quality Range'
    END as quality_status,
    total_records
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_QUALITY_ANOMALIES
WHERE is_anomaly = TRUE
ORDER BY z_score DESC, processing_date DESC;

-- ========== COMPREHENSIVE ANALYTICS DASHBOARD VIEWS ==========

-- Time series executive dashboard
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_TIME_SERIES_EXECUTIVE_DASHBOARD AS
SELECT 
    'Registration Growth' as metric_category,
    'Monthly' as metric_type,
    rt.time_period as period,
    rt.registrations::STRING as value,
    COALESCE(rt.growth_rate, 0) as growth_rate,
    rt.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS rt
WHERE rt.period_type = 'monthly'

UNION ALL

SELECT 
    'Geographic Expansion',
    'Top Countries',
    ge.country,
    ge.user_count::STRING,
    ge.user_acquisition_rate,
    ge.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION ge
WHERE ge.user_count_rank <= 5

UNION ALL

SELECT 
    'Data Quality',
    'Trend',
    qt.processing_date::STRING,
    qt.avg_quality_score::STRING,
    COALESCE(qt.quality_change, 0),
    qt.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS qt
ORDER BY processing_date DESC;

-- Anomaly alert dashboard
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_ANOMALY_ALERT_DASHBOARD AS
SELECT 
    'Registration' as anomaly_category,
    ra.time_period as identifier,
    ra.anomaly_severity as severity,
    ra.anomaly_type as type,
    ra.z_score,
    ra.registrations as metric_value,
    ra.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_ANOMALIES ra
WHERE ra.is_anomaly = TRUE AND ra.anomaly_severity IN ('Critical', 'High')

UNION ALL

SELECT 
    'Data Quality',
    qa.processing_date::STRING,
    qa.anomaly_severity,
    qa.anomaly_type,
    qa.z_score,
    qa.avg_quality_score,
    qa.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_QUALITY_ANOMALIES qa
WHERE qa.is_anomaly = TRUE AND qa.anomaly_severity IN ('Critical', 'High')
ORDER BY processing_date DESC, z_score DESC;

-- ========== BUSINESS INTELLIGENCE SUMMARY VIEWS ==========

-- Monthly business summary
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_MONTHLY_BUSINESS_SUMMARY AS
SELECT 
    rt.time_period as month,
    rt.registrations as new_registrations,
    rt.growth_rate as registration_growth_rate,
    rt.countries_represented as countries_active,
    ROUND(rt.avg_age_at_registration, 1) as avg_new_user_age,
    qt.avg_quality_score as data_quality_score,
    qt.quality_trend as quality_direction,
    CASE 
        WHEN rt.growth_rate > 0 AND qt.quality_trend = 'Improving' THEN 'Excellent'
        WHEN rt.growth_rate > 0 OR qt.quality_trend = 'Improving' THEN 'Good'
        WHEN rt.growth_rate < -10 OR qt.quality_trend = 'Declining' THEN 'Needs Attention'
        ELSE 'Stable'
    END as overall_performance,
    rt.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_REGISTRATION_TRENDS rt
LEFT JOIN ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_DATA_QUALITY_TRENDS qt 
    ON rt.processing_date = qt.processing_date
WHERE rt.period_type = 'monthly'
ORDER BY rt.time_period DESC;

-- Geographic expansion insights
CREATE OR REPLACE VIEW ${SNOWFLAKE_DATABASE}.GOLD_LAYER.VW_GEOGRAPHIC_EXPANSION_INSIGHTS AS
SELECT 
    ge.expansion_category,
    COUNT(*) as countries_in_category,
    SUM(ge.user_count) as total_users,
    ROUND(AVG(ge.user_acquisition_rate), 4) as avg_acquisition_rate,
    ROUND(AVG(ge.market_share_percentage), 2) as avg_market_share,
    MIN(ge.first_user_date) as earliest_entry_date,
    MAX(ge.first_user_date) as latest_entry_date,
    ROUND(AVG(ge.avg_user_age), 1) as avg_user_age_in_category,
    ge.processing_date
FROM ${SNOWFLAKE_DATABASE}.SILVER_LAYER.FACT_GEOGRAPHIC_EXPANSION ge
GROUP BY ge.expansion_category, ge.processing_date
ORDER BY total_users DESC; 
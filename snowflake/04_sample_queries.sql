-- Sample Analytics Queries for Pipeline Data
-- This script demonstrates various analytical queries on the external tables

USE DATABASE ECOMMERCE_DB;
USE SCHEMA BRONZE_LAYER;

-- =====================================================
-- BASIC DATA EXPLORATION
-- =====================================================
SELECT * FROM ext_raw_users 
ORDER BY registered_date DESC 
LIMIT 10;

-- Count total users processed
SELECT COUNT(*) as total_users FROM ext_raw_users;

-- =====================================================
-- GENDER ANALYTICS
-- =====================================================

-- Gender distribution from analytics table
SELECT 
  metric_name,
  metric_value as count,
  percentage
FROM ext_user_analytics 
WHERE metric_type = 'gender_distribution'
ORDER BY metric_value DESC;

-- Gender distribution calculated directly from raw data
SELECT 
  gender,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ext_raw_users), 2) as percentage
FROM ext_raw_users
GROUP BY gender
ORDER BY count DESC;

-- =====================================================
-- GEOGRAPHIC ANALYTICS
-- =====================================================

-- Top 10 countries by user count
SELECT 
  demographic_value as country,
  user_count,
  percentage
FROM ext_user_demographics
WHERE demographic_type = 'country_distribution'
ORDER BY user_count DESC
LIMIT 10;

-- Top 10 states by user count
SELECT 
  demographic_value as state,
  user_count,
  percentage
FROM ext_user_demographics
WHERE demographic_type = 'state_distribution'
ORDER BY user_count DESC
LIMIT 10;

-- =====================================================
-- EMAIL DOMAIN ANALYTICS
-- =====================================================

-- Top email domains from analytics table
SELECT 
  REPLACE(metric_name, 'domain_', '') as email_domain,
  metric_value as count,
  percentage
FROM ext_user_analytics 
WHERE metric_type = 'email_domain_distribution'
ORDER BY metric_value DESC;

-- Email domain analysis from raw data
SELECT 
  SPLIT_PART(email, '@', 2) as email_domain,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ext_raw_users), 2) as percentage
FROM ext_raw_users
GROUP BY email_domain
ORDER BY count DESC
LIMIT 10;

-- =====================================================
-- TIME-BASED ANALYTICS
-- =====================================================

-- User registration trends by year
SELECT 
  YEAR(TO_TIMESTAMP(registered_date)) as registration_year,
  COUNT(*) as user_count
FROM ext_raw_users
GROUP BY registration_year
ORDER BY registration_year;

-- Age distribution (calculated from date of birth)
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

-- =====================================================
-- COMBINED ANALYTICS
-- =====================================================

-- Gender distribution by country (top 5 countries)
WITH top_countries AS (
  SELECT demographic_value as country
  FROM ext_user_demographics
  WHERE demographic_type = 'country_distribution'
  ORDER BY user_count DESC
  LIMIT 5
)
SELECT 
  SPLIT_PART(SPLIT_PART(address, ',', -1), ' ', -1) as country,
  gender,
  COUNT(*) as count
FROM ext_raw_users
WHERE SPLIT_PART(SPLIT_PART(address, ',', -1), ' ', -1) IN (SELECT country FROM top_countries)
GROUP BY country, gender
ORDER BY country, count DESC;

-- =====================================================
-- DATA QUALITY CHECKS
-- =====================================================

-- Check for missing or invalid data
SELECT 
  'Missing IDs' as check_type,
  COUNT(*) as count
FROM ext_raw_users 
WHERE id IS NULL OR id = ''

UNION ALL

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
WHERE email NOT LIKE '%@%.%'

UNION ALL

SELECT 
  'Future birth dates' as check_type,
  COUNT(*) as count
FROM ext_raw_users 
WHERE TO_DATE(dob) > CURRENT_DATE();

-- =====================================================
-- PIPELINE MONITORING
-- =====================================================

-- Latest data processing timestamps
SELECT 
  'Analytics' as data_type,
  MAX(created_at) as latest_processing_time,
  COUNT(DISTINCT created_at) as processing_batches
FROM ext_user_analytics

UNION ALL

SELECT 
  'Demographics' as data_type,
  MAX(created_at) as latest_processing_time,
  COUNT(DISTINCT created_at) as processing_batches
FROM ext_user_demographics;

-- Processing volume over time
SELECT 
  DATE(created_at) as processing_date,
  COUNT(*) as records_processed
FROM ext_user_analytics
GROUP BY processing_date
ORDER BY processing_date DESC; 
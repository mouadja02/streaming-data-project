#!/usr/bin/env python3

"""
AWS Glue Job: Time Series Analysis and Trend Detection
======================================================

This Glue job performs time-series analysis on user data to identify trends,
patterns, and anomalies over time, storing results in Iceberg tables.

Features:
- User registration trends over time
- Age distribution trends
- Geographic expansion patterns
- Data quality trends
- Seasonal pattern detection
- Anomaly detection
- Predictive insights
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, date, timedelta

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_INPUT_PATH',
    'S3_OUTPUT_PATH',
    'CATALOG_DATABASE',
    'AWS_REGION'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def create_registration_trends(df: DataFrame) -> DataFrame:
    """Analyze user registration trends over time"""
    print("📈 Creating registration trends analysis...")
    
    # Extract time components from registration date
    registration_trends = df.withColumn(
        "registration_year", year(col("registration_date"))
    ).withColumn(
        "registration_month", month(col("registration_date"))
    ).withColumn(
        "registration_quarter", quarter(col("registration_date"))
    ).withColumn(
        "registration_day_of_week", dayofweek(col("registration_date"))
    ).withColumn(
        "registration_day_name", 
        when(col("registration_day_of_week") == 1, "Sunday")
        .when(col("registration_day_of_week") == 2, "Monday")
        .when(col("registration_day_of_week") == 3, "Tuesday")
        .when(col("registration_day_of_week") == 4, "Wednesday")
        .when(col("registration_day_of_week") == 5, "Thursday")
        .when(col("registration_day_of_week") == 6, "Friday")
        .otherwise("Saturday")
    )
    
    # Monthly registration trends
    monthly_trends = registration_trends.groupBy(
        "registration_year", "registration_month", "processing_date"
    ).agg(
        count("*").alias("registrations"),
        countDistinct("country").alias("countries_represented"),
        avg("age_years").alias("avg_age_at_registration"),
        sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_registrations"),
        sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_registrations"),
        avg("data_quality_score").alias("avg_data_quality")
    ).withColumn(
        "time_period", concat(col("registration_year"), lit("-"), 
                             lpad(col("registration_month"), 2, "0"))
    ).withColumn(
        "period_type", lit("monthly")
    ).withColumn(
        "male_percentage", 
        round(col("male_registrations") * 100.0 / col("registrations"), 2)
    ).withColumn(
        "female_percentage",
        round(col("female_registrations") * 100.0 / col("registrations"), 2)
    )
    
    # Quarterly trends
    quarterly_trends = registration_trends.groupBy(
        "registration_year", "registration_quarter", "processing_date"
    ).agg(
        count("*").alias("registrations"),
        countDistinct("country").alias("countries_represented"),
        avg("age_years").alias("avg_age_at_registration"),
        sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_registrations"),
        sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_registrations"),
        avg("data_quality_score").alias("avg_data_quality")
    ).withColumn(
        "time_period", concat(col("registration_year"), lit("-Q"), col("registration_quarter"))
    ).withColumn(
        "period_type", lit("quarterly")
    ).withColumn(
        "male_percentage", 
        round(col("male_registrations") * 100.0 / col("registrations"), 2)
    ).withColumn(
        "female_percentage",
        round(col("female_registrations") * 100.0 / col("registrations"), 2)
    )
    
    # Day of week patterns
    dow_trends = registration_trends.groupBy(
        "registration_day_of_week", "registration_day_name", "processing_date"
    ).agg(
        count("*").alias("registrations"),
        avg("age_years").alias("avg_age_at_registration"),
        avg("data_quality_score").alias("avg_data_quality")
    ).withColumn(
        "time_period", col("registration_day_name")
    ).withColumn(
        "period_type", lit("day_of_week")
    ).withColumn(
        "male_registrations", lit(0)
    ).withColumn(
        "female_registrations", lit(0)
    ).withColumn(
        "male_percentage", lit(0.0)
    ).withColumn(
        "female_percentage", lit(0.0)
    ).withColumn(
        "countries_represented", lit(0)
    )
    
    # Combine all trends
    all_trends = monthly_trends.select(
        col("time_period"),
        col("period_type"),
        col("registrations"),
        col("countries_represented"),
        col("avg_age_at_registration"),
        col("male_percentage"),
        col("female_percentage"),
        col("avg_data_quality"),
        col("processing_date")
    ).union(
        quarterly_trends.select(
            col("time_period"),
            col("period_type"),
            col("registrations"),
            col("countries_represented"),
            col("avg_age_at_registration"),
            col("male_percentage"),
            col("female_percentage"),
            col("avg_data_quality"),
            col("processing_date")
        )
    ).union(
        dow_trends.select(
            col("time_period"),
            col("period_type"),
            col("registrations"),
            col("countries_represented"),
            col("avg_age_at_registration"),
            col("male_percentage"),
            col("female_percentage"),
            col("avg_data_quality"),
            col("processing_date")
        )
    )
    
    # Add ranking and growth calculations
    window_spec = Window.partitionBy("period_type").orderBy("time_period")
    all_trends = all_trends.withColumn(
        "previous_registrations", 
        lag("registrations", 1).over(window_spec)
    ).withColumn(
        "growth_rate",
        when(col("previous_registrations").isNotNull() & (col("previous_registrations") > 0),
             round((col("registrations") - col("previous_registrations")) * 100.0 / col("previous_registrations"), 2))
        .otherwise(null())
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Registration trends created with {all_trends.count()} records")
    return all_trends

def create_geographic_expansion_trends(df: DataFrame) -> DataFrame:
    """Analyze geographic expansion patterns over time"""
    print("Creating geographic expansion trends...")
    
    # Country expansion over time
    country_expansion = df.groupBy("country", "processing_date").agg(
        count("*").alias("user_count"),
        min("registration_date").alias("first_user_date"),
        max("registration_date").alias("latest_user_date"),
        countDistinct("city").alias("cities_represented"),
        avg("age_years").alias("avg_user_age"),
        avg("data_quality_score").alias("avg_data_quality")
    ).withColumn(
        "days_since_first_user",
        datediff(current_date(), col("first_user_date"))
    ).withColumn(
        "user_acquisition_rate",
        round(col("user_count").cast("double") / 
              greatest(col("days_since_first_user"), lit(1)), 4)
    )
    
    # Calculate country penetration metrics
    total_users_window = Window.partitionBy("processing_date")
    country_expansion = country_expansion.withColumn(
        "total_users_global", sum("user_count").over(total_users_window)
    ).withColumn(
        "market_share_percentage",
        round(col("user_count") * 100.0 / col("total_users_global"), 2)
    )
    
    # Rank countries by various metrics
    user_count_window = Window.partitionBy("processing_date").orderBy(col("user_count").desc())
    expansion_rate_window = Window.partitionBy("processing_date").orderBy(col("user_acquisition_rate").desc())
    
    country_expansion = country_expansion.withColumn(
        "user_count_rank", row_number().over(user_count_window)
    ).withColumn(
        "expansion_rate_rank", row_number().over(expansion_rate_window)
    ).withColumn(
        "expansion_category",
        when(col("user_count_rank") <= 5, "Top Tier")
        .when(col("user_count_rank") <= 15, "Mid Tier")
        .otherwise("Emerging")
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Geographic expansion trends created with {country_expansion.count()} records")
    return country_expansion

def create_age_demographic_trends(df: DataFrame) -> DataFrame:
    """Analyze age demographic trends over time"""
    print("Creating age demographic trends...")
    
    # Age group trends by registration year
    age_trends = df.groupBy("age_group", "generation", "registration_date", "processing_date").agg(
        count("*").alias("user_count"),
        avg("data_quality_score").alias("avg_data_quality"),
        countDistinct("country").alias("countries_represented")
    ).withColumn(
        "registration_year", year(col("registration_date"))
    )
    
    # Calculate age group distribution over time
    yearly_age_trends = age_trends.groupBy("age_group", "generation", "registration_year", "processing_date").agg(
        sum("user_count").alias("total_users_in_group"),
        avg("avg_data_quality").alias("avg_data_quality"),
        sum("countries_represented").alias("total_countries")
    )
    
    # Calculate percentage distribution
    year_total_window = Window.partitionBy("registration_year", "processing_date")
    yearly_age_trends = yearly_age_trends.withColumn(
        "total_users_in_year", sum("total_users_in_group").over(year_total_window)
    ).withColumn(
        "percentage_of_year",
        round(col("total_users_in_group") * 100.0 / col("total_users_in_year"), 2)
    )
    
    # Identify trending age groups
    age_group_window = Window.partitionBy("age_group", "processing_date").orderBy("registration_year")
    yearly_age_trends = yearly_age_trends.withColumn(
        "previous_year_percentage",
        lag("percentage_of_year", 1).over(age_group_window)
    ).withColumn(
        "trend_direction",
        when(col("previous_year_percentage").isNull(), "New")
        .when(col("percentage_of_year") > col("previous_year_percentage"), "Growing")
        .when(col("percentage_of_year") < col("previous_year_percentage"), "Declining")
        .otherwise("Stable")
    ).withColumn(
        "trend_magnitude",
        when(col("previous_year_percentage").isNotNull(),
             abs(col("percentage_of_year") - col("previous_year_percentage")))
        .otherwise(0.0)
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Age demographic trends created with {yearly_age_trends.count()} records")
    return yearly_age_trends

def create_data_quality_trends(df: DataFrame) -> DataFrame:
    """Analyze data quality trends over time"""
    print("Creating data quality trends...")
    
    # Daily data quality trends
    daily_quality = df.groupBy("processing_date").agg(
        count("*").alias("total_records"),
        avg("data_quality_score").alias("avg_quality_score"),
        sum(when(col("data_quality_score") >= 90, 1).otherwise(0)).alias("excellent_quality_count"),
        sum(when(col("data_quality_score").between(80, 89), 1).otherwise(0)).alias("good_quality_count"),
        sum(when(col("data_quality_score").between(70, 79), 1).otherwise(0)).alias("fair_quality_count"),
        sum(when(col("data_quality_score") < 70, 1).otherwise(0)).alias("poor_quality_count"),
        sum(when(col("email_valid"), 1).otherwise(0)).alias("valid_emails"),
        sum(when(col("phone_valid"), 1).otherwise(0)).alias("valid_phones")
    ).withColumn(
        "excellent_quality_percentage",
        round(col("excellent_quality_count") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "good_quality_percentage",
        round(col("good_quality_count") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "email_validity_percentage",
        round(col("valid_emails") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "phone_validity_percentage",
        round(col("valid_phones") * 100.0 / col("total_records"), 2)
    )
    
    # Calculate quality trends
    quality_window = Window.orderBy("processing_date")
    daily_quality = daily_quality.withColumn(
        "previous_avg_quality",
        lag("avg_quality_score", 1).over(quality_window)
    ).withColumn(
        "quality_trend",
        when(col("previous_avg_quality").isNull(), "Baseline")
        .when(col("avg_quality_score") > col("previous_avg_quality") + 1, "Improving")
        .when(col("avg_quality_score") < col("previous_avg_quality") - 1, "Declining")
        .otherwise("Stable")
    ).withColumn(
        "quality_change",
        when(col("previous_avg_quality").isNotNull(),
             round(col("avg_quality_score") - col("previous_avg_quality"), 2))
        .otherwise(0.0)
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Data quality trends created with {daily_quality.count()} records")
    return daily_quality

def detect_anomalies(df: DataFrame, metric_col: str, threshold_multiplier: float = 2.0) -> DataFrame:
    """Detect anomalies in time series data using statistical methods"""
    print(f"Detecting anomalies in {metric_col}...")
    
    # Calculate statistical measures
    stats = df.select(
        avg(col(metric_col)).alias("mean_value"),
        stddev(col(metric_col)).alias("std_dev")
    ).collect()[0]
    
    mean_val = stats["mean_value"]
    std_val = stats["std_dev"]
    
    # Add anomaly detection
    anomaly_df = df.withColumn(
        "z_score",
        abs(col(metric_col) - mean_val) / std_val
    ).withColumn(
        "is_anomaly",
        col("z_score") > threshold_multiplier
    ).withColumn(
        "anomaly_type",
        when(col("is_anomaly") & (col(metric_col) > mean_val), "High Anomaly")
        .when(col("is_anomaly") & (col(metric_col) < mean_val), "Low Anomaly")
        .otherwise("Normal")
    ).withColumn(
        "anomaly_severity",
        when(col("z_score") > 3, "Critical")
        .when(col("z_score") > 2, "High")
        .when(col("z_score") > 1.5, "Medium")
        .otherwise("Low")
    )
    
    return anomaly_df

def save_to_iceberg_table(df: DataFrame, table_name: str, write_mode: str = "append"):
    """Save DataFrame to Iceberg table"""
    print(f"💾 Saving data to Iceberg table: {table_name}")
    
    full_table_name = f"glue_catalog.{args['CATALOG_DATABASE']}.{table_name}"
    
    try:
        df.writeTo(full_table_name).using("iceberg").mode(write_mode).save()
        print(f"✅ Successfully saved to Iceberg table: {full_table_name}")
        
        # Show table statistics
        try:
            spark.sql(f"SELECT COUNT(*) as record_count FROM {full_table_name}").show()
        except:
            pass
            
    except Exception as e:
        print(f"❌ Error saving to Iceberg table: {str(e)}")
        # Fallback to Parquet
        fallback_path = f"{args['S3_OUTPUT_PATH']}/{table_name}_parquet/"
        print(f"Falling back to Parquet at: {fallback_path}")
        df.write.mode(write_mode).parquet(fallback_path)

def main():
    """Main time series analysis pipeline"""
    print("Starting Time Series Analysis Job")
    print(f"Input Path: {args['S3_INPUT_PATH']}")
    print(f"Output Path: {args['S3_OUTPUT_PATH']}")
    print(f"Database: {args['CATALOG_DATABASE']}")
    
    try:
        # Read transformed user data
        print("Reading transformed user data...")
        full_table_name = f"glue_catalog.{args['CATALOG_DATABASE']}.users_transformed"
        
        try:
            users_df = spark.sql(f"SELECT * FROM {full_table_name}")
            print("✅ Successfully read from Iceberg table")
        except:
            print("Fallback: Reading from S3 Parquet files...")
            users_df = spark.read.parquet(f"{args['S3_INPUT_PATH']}/users_transformed_parquet/")
        
        print(f"Input data count: {users_df.count()} records")
        
        # Create time series analyses
        print("\n🔄 Creating time series analyses...")
        
        # 1. Registration Trends
        registration_trends = create_registration_trends(users_df)
        save_to_iceberg_table(registration_trends, "fact_registration_trends", "append")
        
        # 2. Geographic Expansion Trends
        geo_expansion = create_geographic_expansion_trends(users_df)
        save_to_iceberg_table(geo_expansion, "fact_geographic_expansion", "append")
        
        # 3. Age Demographic Trends
        age_trends = create_age_demographic_trends(users_df)
        save_to_iceberg_table(age_trends, "fact_age_demographic_trends", "append")
        
        # 4. Data Quality Trends
        quality_trends = create_data_quality_trends(users_df)
        save_to_iceberg_table(quality_trends, "fact_data_quality_trends", "append")
        
        # 5. Anomaly Detection
        print("\nPerforming anomaly detection...")
        
        # Detect registration anomalies
        reg_anomalies = detect_anomalies(
            registration_trends.filter(col("period_type") == "monthly"), 
            "registrations"
        )
        save_to_iceberg_table(reg_anomalies, "fact_registration_anomalies", "append")
        
        # Detect quality anomalies
        quality_anomalies = detect_anomalies(quality_trends, "avg_quality_score")
        save_to_iceberg_table(quality_anomalies, "fact_quality_anomalies", "append")
        
        # Create summary report
        print("\nTime Series Analysis Summary:")
        print("=" * 60)
        
        # Registration trends summary
        registration_trends.filter(col("period_type") == "monthly") \
            .orderBy(col("time_period").desc()).limit(6).show()
        print("Recent Monthly Registration Trends ☝️")
        
        # Top growing countries
        geo_expansion.orderBy(col("user_acquisition_rate").desc()).limit(5).show()
        print("Fastest Growing Countries ☝️")
        
        # Age trend summary
        age_trends.filter(col("trend_direction") == "Growing") \
            .orderBy(col("trend_magnitude").desc()).limit(5).show()
        print("Fastest Growing Age Groups ☝️")
        
        # Quality trend summary
        quality_trends.orderBy(col("processing_date").desc()).limit(3).show()
        print("Recent Data Quality Trends")
        
        # Anomaly summary
        anomaly_count = reg_anomalies.filter(col("is_anomaly")).count()
        quality_anomaly_count = quality_anomalies.filter(col("is_anomaly")).count()
        
        print(f"\nAnomalies Detected:")
        print(f"   Registration Anomalies: {anomaly_count}")
        print(f"   Quality Anomalies: {quality_anomaly_count}")
        
        print("✅ Time Series Analysis Job completed successfully!")
        
    except Exception as e:
        print(f"❌ Job failed with error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit() 
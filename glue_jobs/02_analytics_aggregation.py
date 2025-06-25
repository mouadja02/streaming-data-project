#!/usr/bin/env python3

"""
AWS Glue Job: Advanced Analytics Aggregation to Iceberg
=======================================================

This Glue job creates advanced analytics aggregations from the transformed user data,
building dimensional tables and metrics for business intelligence and reporting.

Features:
- Demographic analysis and segmentation
- Time-based analytics and trends
- Geographic distribution analysis
- Email and communication preferences
- Data quality metrics and monitoring
- Slowly Changing Dimensions (SCD) support
- Iceberg tables with time travel capabilities
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
import boto3
from datetime import datetime, date

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

# Configure Spark for Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args['S3_OUTPUT_PATH'])
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def create_user_demographics_dim(df: DataFrame) -> DataFrame:
    """
    Create user demographics dimension table
    """
    print("👥 Creating user demographics dimension...")
    
    demographics_dim = df.select(
        col("id").alias("user_id"),
        col("age_group"),
        col("generation"),
        col("gender"),
        col("country"),
        col("city"),
        col("email_provider_type"),
        col("account_tenure_category"),
        col("data_quality_score"),
        col("processing_date")
    ).distinct()
    
    # Add demographic scoring
    demographics_dim = demographics_dim.withColumn(
        "demographic_score",
        when(col("age_group").isin("25-34", "35-44"), 10).otherwise(5) +
        when(col("generation").isin("Millennial", "Gen X"), 10).otherwise(5) +
        when(col("email_provider_type") == "Business", 15).otherwise(5) +
        when(col("account_tenure_category") == "Veteran", 20).otherwise(10)
    ).withColumn(
        "target_segment",
        when(col("demographic_score") >= 40, "Premium")
        .when(col("demographic_score") >= 25, "Standard")
        .otherwise("Basic")
    )
    
    # Add metadata
    demographics_dim = demographics_dim.withColumn(
        "dim_created_at", current_timestamp()
    ).withColumn(
        "dim_updated_at", current_timestamp()
    ).withColumn(
        "is_current", lit(True)
    )
    
    print(f"✅ Demographics dimension created with {demographics_dim.count()} records")
    return demographics_dim

def create_geographic_analysis(df: DataFrame) -> DataFrame:
    """
    Create geographic analysis aggregations
    """
    print("Creating geographic analysis...")
    
    # Country-level aggregations
    country_stats = df.groupBy("country", "processing_date").agg(
        count("*").alias("user_count"),
        countDistinct("email_domain").alias("unique_email_domains"),
        avg("age_years").alias("avg_age"),
        avg("data_quality_score").alias("avg_data_quality"),
        sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_count"),
        sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_count"),
        sum(when(col("email_provider_type") == "Business", 1).otherwise(0)).alias("business_email_count")
    ).withColumn(
        "male_percentage", 
        round(col("male_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "female_percentage",
        round(col("female_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "business_email_percentage",
        round(col("business_email_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "analysis_type", lit("country")
    ).withColumn(
        "geographic_level", col("country")
    )
    
    # City-level aggregations
    city_stats = df.groupBy("city", "country", "processing_date").agg(
        count("*").alias("user_count"),
        countDistinct("email_domain").alias("unique_email_domains"),
        avg("age_years").alias("avg_age"),
        avg("data_quality_score").alias("avg_data_quality"),
        sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_count"),
        sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_count"),
        sum(when(col("email_provider_type") == "Business", 1).otherwise(0)).alias("business_email_count")
    ).withColumn(
        "male_percentage", 
        round(col("male_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "female_percentage",
        round(col("female_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "business_email_percentage",
        round(col("business_email_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "analysis_type", lit("city")
    ).withColumn(
        "geographic_level", concat_ws(", ", col("city"), col("country")))
    
    # Combine geographic stats
    geographic_analysis = country_stats.select(
        col("geographic_level"),
        col("analysis_type"),
        col("user_count"),
        col("unique_email_domains"),
        col("avg_age"),
        col("avg_data_quality"),
        col("male_percentage"),
        col("female_percentage"),
        col("business_email_percentage"),
        col("processing_date")
    ).union(
        city_stats.select(
            col("geographic_level"),
            col("analysis_type"),
            col("user_count"),
            col("unique_email_domains"),
            col("avg_age"),
            col("avg_data_quality"),
            col("male_percentage"),
            col("female_percentage"),
            col("business_email_percentage"),
            col("processing_date")
        )
    )
    
    # Add ranking
    window = Window.partitionBy("analysis_type", "processing_date").orderBy(col("user_count").desc())
    geographic_analysis = geographic_analysis.withColumn(
        "user_count_rank", row_number().over(window)
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Geographic analysis created with {geographic_analysis.count()} records")
    return geographic_analysis

def create_age_generation_analysis(df: DataFrame) -> DataFrame:
    """
    Create age and generation analysis
    """
    print("Creating age and generation analysis...")
    
    age_gen_analysis = df.groupBy("age_group", "generation", "gender", "processing_date").agg(
        count("*").alias("user_count"),
        avg("data_quality_score").alias("avg_data_quality"),
        avg("account_age_days").alias("avg_account_age_days"),
        countDistinct("country").alias("countries_represented"),
        countDistinct("email_domain").alias("unique_email_domains"),
        sum(when(col("email_provider_type") == "Personal", 1).otherwise(0)).alias("personal_email_count"),
        sum(when(col("email_provider_type") == "Business", 1).otherwise(0)).alias("business_email_count"),
        sum(when(col("account_tenure_category") == "Veteran", 1).otherwise(0)).alias("veteran_users"),
        sum(when(col("account_tenure_category") == "New", 1).otherwise(0)).alias("new_users")
    )
    
    # Calculate percentages and insights
    total_users_window = Window.partitionBy("processing_date")
    age_gen_analysis = age_gen_analysis.withColumn(
        "total_users_in_batch", sum("user_count").over(total_users_window)
    ).withColumn(
        "percentage_of_total", 
        round(col("user_count") * 100.0 / col("total_users_in_batch"), 2)
    ).withColumn(
        "business_email_percentage",
        round(col("business_email_count") * 100.0 / col("user_count"), 2)
    ).withColumn(
        "veteran_percentage",
        round(col("veteran_users") * 100.0 / col("user_count"), 2)
    )
    
    # Add engagement scoring
    age_gen_analysis = age_gen_analysis.withColumn(
        "engagement_score",
        when(col("avg_account_age_days") > 365, 20).otherwise(10) +
        when(col("business_email_percentage") > 50, 15).otherwise(5) +
        when(col("avg_data_quality") > 80, 10).otherwise(5) +
        when(col("countries_represented") > 1, 5).otherwise(0)
    ).withColumn(
        "engagement_tier",
        when(col("engagement_score") >= 35, "High")
        .when(col("engagement_score") >= 20, "Medium")
        .otherwise("Low")
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Age/Generation analysis created with {age_gen_analysis.count()} records")
    return age_gen_analysis

def create_email_communication_analysis(df: DataFrame) -> DataFrame:
    """
    Create email and communication preferences analysis
    """
    print("Creating email communication analysis...")
    
    # Email domain analysis
    email_domain_analysis = df.groupBy("email_domain", "email_provider_type", "processing_date").agg(
        count("*").alias("user_count"),
        countDistinct("country").alias("countries_using"),
        avg("age_years").alias("avg_user_age"),
        avg("data_quality_score").alias("avg_data_quality"),
        sum(when(col("gender") == "male", 1).otherwise(0)).alias("male_users"),
        sum(when(col("gender") == "female", 1).otherwise(0)).alias("female_users")
    )
    
    # Calculate market share
    total_users_window = Window.partitionBy("processing_date")
    email_domain_analysis = email_domain_analysis.withColumn(
        "total_users_in_batch", sum("user_count").over(total_users_window)
    ).withColumn(
        "market_share_percentage",
        round(col("user_count") * 100.0 / col("total_users_in_batch"), 2)
    ).withColumn(
        "gender_ratio_male_female",
        when(col("female_users") > 0, 
             round(col("male_users").cast("double") / col("female_users"), 2))
        .otherwise(lit(None)) 
    )
    
    # Provider type summary
    provider_summary = df.groupBy("email_provider_type", "processing_date").agg(
        count("*").alias("total_users"),
        countDistinct("email_domain").alias("unique_domains"),
        avg("age_years").alias("avg_age"),
        avg("data_quality_score").alias("avg_data_quality"),
        countDistinct("country").alias("countries_represented")
    ).withColumn(
        "analysis_level", lit("provider_type")
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    # Domain-level details
    domain_details = email_domain_analysis.withColumn(
        "analysis_level", lit("domain")
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Email communication analysis created")
    return provider_summary, domain_details

def create_data_quality_metrics(df: DataFrame) -> DataFrame:
    """
    Create comprehensive data quality metrics
    """
    print("Creating data quality metrics...")
    
    # Overall quality metrics
    quality_metrics = df.agg(
        count("*").alias("total_records"),
        sum(when(col("data_quality_score") >= 90, 1).otherwise(0)).alias("excellent_quality"),
        sum(when(col("data_quality_score").between(80, 89), 1).otherwise(0)).alias("good_quality"),
        sum(when(col("data_quality_score").between(70, 79), 1).otherwise(0)).alias("fair_quality"),
        sum(when(col("data_quality_score").between(50, 69), 1).otherwise(0)).alias("poor_quality"),
        sum(when(col("data_quality_score") < 50, 1).otherwise(0)).alias("very_poor_quality"),
        avg("data_quality_score").alias("avg_quality_score"),
        min("data_quality_score").alias("min_quality_score"),
        max("data_quality_score").alias("max_quality_score"),
        sum(when(col("email_valid"), 1).otherwise(0)).alias("valid_emails"),
        sum(when(col("phone_valid"), 1).otherwise(0)).alias("valid_phones"),
        countDistinct("id").alias("unique_users"),
        sum(when(size(col("quality_issues")) > 0, 1).otherwise(0)).alias("records_with_issues")
    ).withColumn(
        "processing_date", current_date()
    ).withColumn(
        "excellent_quality_percentage", 
        round(col("excellent_quality") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "good_quality_percentage",
        round(col("good_quality") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "email_validity_percentage",
        round(col("valid_emails") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "phone_validity_percentage",
        round(col("valid_phones") * 100.0 / col("total_records"), 2)
    ).withColumn(
        "data_completeness_score",
        round((col("valid_emails") + col("valid_phones")) * 100.0 / (col("total_records") * 2), 2)
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    # Quality by demographic segments
    quality_by_segment = df.groupBy("age_group", "generation", "processing_date").agg(
        count("*").alias("segment_total"),
        avg("data_quality_score").alias("avg_segment_quality"),
        sum(when(col("email_valid"), 1).otherwise(0)).alias("valid_emails_in_segment"),
        sum(when(col("phone_valid"), 1).otherwise(0)).alias("valid_phones_in_segment")
    ).withColumn(
        "segment_email_validity_rate",
        round(col("valid_emails_in_segment") * 100.0 / col("segment_total"), 2)
    ).withColumn(
        "segment_phone_validity_rate",
        round(col("valid_phones_in_segment") * 100.0 / col("segment_total"), 2)
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    print(f"✅ Data quality metrics created")
    return quality_metrics, quality_by_segment

def save_to_iceberg_table(df: DataFrame, table_name: str, write_mode: str = "append"):
    """
    Save DataFrame to Iceberg table with proper configuration
    """
    print(f"Saving data to Iceberg table: {table_name}")
    
    # Map table names to S3 locations to match catalog setup
    table_locations = {
        "dim_user_demographics": f"{args['S3_OUTPUT_PATH']}/dim_user_demographics/",
        "fact_geographic_analysis": f"{args['S3_OUTPUT_PATH']}/fact_geographic_analysis/",
        "fact_age_generation_analysis": f"{args['S3_OUTPUT_PATH']}/fact_age_generation_analysis/",
        "fact_email_provider_analysis": f"{args['S3_OUTPUT_PATH']}/fact_email_provider_analysis/",
        "fact_email_domain_analysis": f"{args['S3_OUTPUT_PATH']}/fact_email_domain_analysis/",
        "fact_data_quality_metrics": f"{args['S3_OUTPUT_PATH']}/fact_data_quality_metrics/",
        "fact_quality_by_segment": f"{args['S3_OUTPUT_PATH']}/fact_quality_by_segment/"
    }
    
    # Get the S3 location for this table
    s3_location = table_locations.get(table_name, f"{args['S3_OUTPUT_PATH']}/{table_name}/")
    
    full_table_name = f"glue_catalog.{args['CATALOG_DATABASE']}.{table_name}"
    
    try:
        # Configure Iceberg table properties
        iceberg_options = {
            "path": s3_location,
            "catalog-name": "glue_catalog",
            "warehouse": args['S3_OUTPUT_PATH'],
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
        
        # Write to Iceberg table with explicit location
        writer = df.write.format("iceberg")
        for key, value in iceberg_options.items():
            writer = writer.option(key, value)
        
        # For SCD tables, use overwrite for dimensions
        if "dim_" in table_name and write_mode == "merge":
            writer.mode("overwrite").saveAsTable(full_table_name)
        else:
            writer.mode(write_mode).saveAsTable(full_table_name)
            
        print(f"✅ Successfully saved to Iceberg table: {full_table_name}")
        print(f"Location: {s3_location}")
        
        # Show table statistics
        try:
            spark.sql(f"SELECT COUNT(*) as record_count FROM {full_table_name}").show()
        except:
            print("Could not query table (normal for first run)")
            
    except Exception as e:
        print(f"❌ Error saving to Iceberg table: {str(e)}")
        print(f"🔄 Attempting alternative Iceberg write method...")
        
        try:
            # Alternative method: Create table first, then insert
            df.createOrReplaceTempView("temp_insert_view")
            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name}
                USING ICEBERG
                LOCATION '{s3_location}'
                AS SELECT * FROM temp_insert_view WHERE 1=0
            """)
            
            # Insert data
            if write_mode == "overwrite" or ("dim_" in table_name and write_mode == "merge"):
                spark.sql(f"DELETE FROM {full_table_name}")
            spark.sql(f"INSERT INTO {full_table_name} SELECT * FROM temp_insert_view")
            
            print(f"✅ Successfully saved using alternative method")
            
        except Exception as e2:
            print(f"❌ Alternative method also failed: {str(e2)}")
            # Final fallback to Parquet
            fallback_path = f"{args['S3_OUTPUT_PATH']}/{table_name}_parquet/"
            print(f"Final fallback to Parquet at: {fallback_path}")
            df.write.mode(write_mode).parquet(fallback_path)

def main():
    """
    Main analytics aggregation pipeline
    """
    print("Starting Advanced Analytics Aggregation Job")
    print(f"Input Path: {args['S3_INPUT_PATH']}")
    print(f"Output Path: {args['S3_OUTPUT_PATH']}")
    print(f"Database: {args['CATALOG_DATABASE']}")
    
    try:
        # Read transformed user data
        print("Reading transformed user data...")
        full_table_name = f"glue_catalog.{args['CATALOG_DATABASE']}.users_transformed"
        
        try:
            # Try to read from Iceberg table first
            users_df = spark.sql(f"SELECT * FROM {full_table_name}")
            print("✅ Successfully read from Iceberg table")
        except:
            # Fallback to S3 Parquet files
            print("Fallback: Reading from S3 Parquet files...")
            users_df = spark.read.parquet(f"{args['S3_INPUT_PATH']}/iceberg-warehouse/users_cleaned_transformed/data/")
        
        print(f"Input data count: {users_df.count()} records")
        
        # Create analytics tables
        print("\n🔄 Creating analytics aggregations...")
        
        # 1. User Demographics Dimension
        demographics_dim = create_user_demographics_dim(users_df)
        save_to_iceberg_table(demographics_dim, "dim_user_demographics", "overwrite")
        
        # 2. Geographic Analysis
        geographic_analysis = create_geographic_analysis(users_df)
        save_to_iceberg_table(geographic_analysis, "fact_geographic_analysis", "append")
        
        # 3. Age/Generation Analysis
        age_gen_analysis = create_age_generation_analysis(users_df)
        save_to_iceberg_table(age_gen_analysis, "fact_age_generation_analysis", "append")
        
        # 4. Email Communication Analysis
        provider_summary, domain_details = create_email_communication_analysis(users_df)
        save_to_iceberg_table(provider_summary, "fact_email_provider_analysis", "append")
        save_to_iceberg_table(domain_details, "fact_email_domain_analysis", "append")
        
        # 5. Data Quality Metrics
        quality_metrics, quality_by_segment = create_data_quality_metrics(users_df)
        save_to_iceberg_table(quality_metrics, "fact_data_quality_metrics", "append")
        save_to_iceberg_table(quality_by_segment, "fact_quality_by_segment", "append")
        
        # Create summary report
        print("\nAnalytics Summary Report:")
        print("=" * 50)
        
        demographics_dim.groupBy("target_segment").count().show()
        print("Target Segment Distribution ☝️")
        
        geographic_analysis.filter(col("analysis_type") == "country") \
            .orderBy(col("user_count").desc()).limit(5).show()
        print("Top 5 Countries by User Count ☝️")
        
        age_gen_analysis.groupBy("engagement_tier").count().show()
        print("Engagement Tier Distribution ☝️")
        
        provider_summary.orderBy(col("total_users").desc()).show()
        print("Email Provider Analysis ☝️")
        
        quality_metrics.select(
            "total_records", "avg_quality_score", 
            "excellent_quality_percentage", "email_validity_percentage"
        ).show()
        print("Data Quality Summary ☝️")
        
        print("✅ Advanced Analytics Aggregation Job completed successfully!")
        
    except Exception as e:
        print(f"❌ Job failed with error: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
    job.commit() 
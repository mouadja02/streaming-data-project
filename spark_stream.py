import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,'
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk-bundle:1.11.901') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('maxOffsetsPerTrigger', 1000) \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("title", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return sel


def process_batch(df, epoch_id):
    """
    Processes each micro-batch of data.
    - Repartitions the DataFrame to control the size of the output files.
    - Writes the data to S3 in Parquet format.
    """
    num_records = df.count()
    if num_records > 0:
        records_per_file = 100
        # Perform integer ceiling division to determine the number of partitions
        num_partitions = (num_records + records_per_file - 1) // records_per_file

        logging.info(f"Epoch {epoch_id}: Writing {num_records} records to S3 in {num_partitions} files.")

        (df.repartition(num_partitions)
         .write
         .mode("append")
         .parquet("s3://my-amazing-app/users/"))


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        
        logging.info("Streaming is being started...")

        streaming_query = (selection_df.writeStream
                           .foreachBatch(process_batch)
                           .outputMode("update")
                           .option('checkpointLocation', './tmp/checkpoint')
                           .trigger(processingTime='10 seconds')
                           .start())

        streaming_query.awaitTermination()

import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode

# Set up necessary packages
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk:1.11.375 pyspark-shell'

# Get AWS Credentials
session = boto3.Session(profile_name='default')
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()

def main():
    # Create a Spark session with AWS credentials
    spark = SparkSession.builder \
        .appName("S3DataProcessor") \
        .config("spark.hadoop.fs.s3a.access.key", current_credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", current_credentials.secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Read one-way data from the directory containing all one-way JSON files
    one_way_df = spark.read.json("s3a://bestpricenphu/data/raw/kayak/one-way/")

    # Transform one-way data
    transformed_one_way_df = transform_one_way_data(one_way_df)

    # Read round-trip data from the directory containing all round-trip JSON files
    round_trip_df = spark.read.json("s3a://bestpricenphu/data/raw/kayak/round-way/")

    # Transform round-trip data
    transformed_round_trip_df = transform_round_trip_data(round_trip_df)

    # Write transformed data to PostgreSQL and Elasticsearch
    write_to_postgres(transformed_one_way_df, "one_way")
    write_to_postgres(transformed_round_trip_df, "round_trip")
    write_to_elasticsearch(transformed_one_way_df, "one_way_index")
    write_to_elasticsearch(transformed_round_trip_df, "round_trip_index")
    
def transform_one_way_data(df):
    # Remove $ from price and convert to double, extract number of stops as integer
    return df.withColumn("price", regexp_replace("price", "\\$", "").cast("double")) \
        .withColumn("num_stops", split("num_stops", " ")[0].cast("int"))

def transform_round_trip_data(df):
    # Remove $ from price and convert to double, remove the nested flights array, extract number of stops as integer
    return df.withColumn("price", regexp_replace("price", "\\$", "").cast("double")) \
        .withColumn("flights", explode("flights")) \
        .select("price", "flights.*") \
        .withColumn("num_stops", split("num_stops", " ")[0].cast("int"))

def write_to_postgres(df, table_name):
    # Write DataFrame to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/mydatabase") \
        .option("dbtable", table_name) \
        .option("user", "nphu01") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()

def write_to_elasticsearch(df, index_name):
    # Write DataFrame to Elasticsearch
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .mode("overwrite") \
        .save(index_name)

if __name__ == "__main__":
    main()

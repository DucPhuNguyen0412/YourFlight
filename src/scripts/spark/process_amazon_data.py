import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract
import boto3
from botocore.exceptions import NoCredentialsError

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk:1.11.375 pyspark-shell'
session = boto3.Session(profile_name='default')
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()

def process_amazon_data_main(model):
    spark = SparkSession.builder \
        .appName('Amazon Data Processing') \
        .config("spark.hadoop.fs.s3a.access.key", current_credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", current_credentials.secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Format model string for use in file paths
    model_formatted = model.replace(' ', '_')

    # Read the CSV data from S3 bucket
    df = spark.read.csv(f's3a://bestpricenphu/data/raw/amazon_data_{model_formatted}.csv', header=True, inferSchema=True)

    # Perform data cleaning and processing here...

    # For example, removing null values where all columns are null:
    df = df.dropna(how='all')

    # Splitting price and reviews by comma and keeping the first value
    df = df.withColumn('price', split(col('price'), ',')[0])
    df = df.withColumn('reviews', split(col('reviews'), ',')[0])

    # Extracting numeric rating from 'rating'
    df = df.withColumn('rating', regexp_extract(col('rating'), r'(\d+\.\d+)', 0).cast('float'))

    # Define base parquet file path in S3 bucket
    base_parquet_dir = 's3a://bestpricenphu/data/processed/amazon'

    # Check if the directory exists, if not, create it
    os.makedirs(base_parquet_dir, exist_ok=True)

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define new parquet file path in S3 bucket
    parquet_file = os.path.join(base_parquet_dir, f'amazon_data_{model_formatted}_{timestamp}.parquet')

    # Delete any old parquet files
    for f in os.listdir(base_parquet_dir):
        if f.startswith(f'amazon_data_{model_formatted}') and f.endswith('.parquet'):
            os.remove(os.path.join(base_parquet_dir, f))

    # Saving the processed data
    df.write.parquet(parquet_file)

if __name__ == "__main__":
    model = sys.argv[1]  # Get model name from command line argument
    process_amazon_data_main(model)

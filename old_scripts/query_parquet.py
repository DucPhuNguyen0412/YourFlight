from pyspark.sql import SparkSession
import sys
import boto3
from botocore.exceptions import NoCredentialsError

def main(model):
    # Get credentials
    session = boto3.Session(profile_name='default')
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()

    # Initialize a Spark Session with S3 access
    spark = SparkSession.builder \
        .appName('QueryParquet') \
        .config("spark.hadoop.fs.s3a.access.key", current_credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", current_credentials.secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Format model string for use in file paths
    model_formatted = model.replace(' ', '_')

    # Define base parquet file paths in S3
    base_parquet_dir_amazon = 's3a://bestpricenphu/dataa/processed/amazon/'
    base_parquet_dir_ebay = 's3a://bestpricenphu/data/processed/ebay/'

    # Parquet file paths for this model
    parquet_file_amazon = base_parquet_dir_amazon + f'amazon_data_{model_formatted}.parquet'
    parquet_file_ebay = base_parquet_dir_ebay + f'ebay_data_{model_formatted}.parquet'

    # Read parquet files
    df_amazon = spark.read.parquet(parquet_file_amazon)
    df_ebay = spark.read.parquet(parquet_file_ebay)

    # Create temporary views
    df_amazon.createOrReplaceTempView("view_amazon")
    df_ebay.createOrReplaceTempView("view_ebay")

    # Query for this model
    query_amazon = f"SELECT model, title, price, rating, reviews FROM view_amazon WHERE model = '{model}'"
    query_ebay = f"SELECT model, title, price, rating, reviews FROM view_ebay WHERE model = '{model}'"

    result_df_amazon = spark.sql(query_amazon)
    result_df_ebay = spark.sql(query_ebay)

    # Show the query results for this model
    print(f"Amazon query result for model '{model}':")
    result_df_amazon.show(40)

    print(f"eBay query result for model '{model}':")
    result_df_ebay.show(40)

if __name__ == "__main__":
    model = sys.argv[1]  # Get model name from command line argument
    main(model)

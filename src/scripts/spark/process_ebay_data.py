import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract

def process_ebay_data_main(model):
    # Initialize a Spark Session
    spark = SparkSession.builder \
        .appName('eBay Data Processing') \
        .getOrCreate()

    # Format model string for use in file paths
    model_formatted = model.replace(' ', '_')

    # Read the CSV data
    df = spark.read.csv(f'/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/ebay_data_{model_formatted}.csv', header=True, inferSchema=True)

    # Perform data cleaning and processing here...

    # For example, removing null values where all columns are null:
    df = df.dropna(how='all')

    # Splitting price and reviews by comma and keeping the first value
    df = df.withColumn('price', split(col('price'), ',')[0])
    df = df.withColumn('reviews', split(col('reviews'), ',')[0])

    # Extracting numeric rating from 'rating'
    df = df.withColumn('rating', regexp_extract(col('rating'), r'(\d+\.\d+)', 0).cast('float'))

    # Define base parquet file path
    base_parquet_dir = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/ebay'

    # Check if the directory exists, if not, create it
    os.makedirs(base_parquet_dir, exist_ok=True)

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define new parquet file path
    parquet_file = os.path.join(base_parquet_dir, f'ebay_data_{model_formatted}_{timestamp}.parquet')

    # Delete any old parquet files
    for f in os.listdir(base_parquet_dir):
        if f.startswith(f'ebay_data_{model_formatted}') and f.endswith('.parquet'):
            os.remove(os.path.join(base_parquet_dir, f))

    # Saving the processed data
    df.write.parquet(parquet_file)

if __name__ == "__main__":
    model = sys.argv[1]  # Get model name from command line argument
    process_ebay_data_main(model)

import datetime
import shutil
import os
import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

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

    # For example, removing null values:
    df = df.dropna()

    # You could perform some transformations, like extracting numeric value from 'reviews'
    df = df.withColumn('reviews', col('reviews').substr(1, 3).cast('float'))

    # Define base parquet file path
    base_parquet_dir = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/'

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define new parquet file path
    parquet_file = os.path.join(base_parquet_dir, f'ebay_data_{model_formatted}_{timestamp}.parquet')

    # Saving the processed data
    df.write.parquet(parquet_file)

if __name__ == "__main__":
    model = sys.argv[1]  # Get model name from command line argument
    process_ebay_data_main(model)

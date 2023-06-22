import datetime
import shutil
import os
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

def process_amazon_data_main():
    # Initialize a Spark Session
    spark = SparkSession.builder \
        .appName('Amazon Data Processing') \
        .getOrCreate()

    # Read the CSV data
    df = spark.read.csv('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/amazon_data.csv', header=True, inferSchema=True)

    # Perform data cleaning and processing here...

    # For example, removing null values:
    df = df.dropna()

    # You could perform some transformations, like extracting numeric value from 'rating'
    df = df.withColumn('rating', col('rating').substr(1, 3).cast('float'))

    # Define base parquet file path
    base_parquet_dir = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/'

    # Delete the older parquet folders if they exist
    for item in os.listdir(base_parquet_dir):
        if 'amazon_data_' in item:
            shutil.rmtree(os.path.join(base_parquet_dir, item), ignore_errors=True)

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define new parquet file path
    parquet_file = os.path.join(base_parquet_dir, f'amazon_data_{timestamp}.parquet')

    # Saving the processed data
    df.write.parquet(parquet_file)

    # Save parquet_file path to a text file
    with open(os.path.join(base_parquet_dir, 'parquet_file_path.txt'), 'w') as f:
        f.write(parquet_file)

if __name__ == "__main__":
    process_amazon_data_main()

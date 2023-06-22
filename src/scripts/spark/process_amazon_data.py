from pyspark import SparkSession
from pyspark.sql.functions import col

import sys
sys.path.append('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/envlibs/lib/python3.10/site-packages')

def process_data_main():
    # Initialize a Spark Session
    spark = SparkSession.builder \
        .appName('Amazon Data Processing') \
        .getOrCreate()

    # Read the CSV data
    df = spark.read.csv('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/amazon_data.csv', header=True, inferSchema=True)

    # Filter the DataFrame for the model "playstation 4"
    df = df.filter(col('model') == 'playstation 4')

    # Perform data cleaning and processing here...

    # For example, removing null values:
    df = df.dropna()

    # You could perform some transformations, like extracting numeric value from 'rating'
    df = df.withColumn('rating', col('rating').substr(1, 3).cast('float'))

    # Saving the processed data
    df.write.parquet('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/amazon_data.parquet')

if __name__ == "__main__":
    process_data_main()

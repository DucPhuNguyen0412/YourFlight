import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

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

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define parquet file path
    parquet_file = f'/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/amazon_data_{timestamp}.parquet'

    # Saving the processed data
    df.write.parquet(parquet_file)

    # Save parquet_file path to a text file
    with open('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/parquet_file_path.txt', 'w') as f:
        f.write(parquet_file)

if __name__ == "__main__":
    process_data_main()

import datetime
from pyspark.sql.functions import col, split, regexp_extract

# Databricks automatically creates a SparkSession called spark.
# Hence, we do not need to create a SparkSession ourselves.
# spark = SparkSession.builder ...

def process_amazon_data_main(model):
    # Format model string for use in file paths
    model_formatted = model.replace(' ', '_')

    # Read the CSV data from S3 bucket
    df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(f'dbfs:/mnt/bestpricenphu/data/raw/amazon_data_{model_formatted}.csv')

    # Perform data cleaning and processing here...
    df = df.dropna(how='all')

    # Splitting price and reviews by comma and keeping the first value
    df = df.withColumn('price', split(col('price'), ',')[0])
    df = df.withColumn('reviews', split(col('reviews'), ',')[0])

    # Extracting numeric rating from 'rating'
    df = df.withColumn('rating', regexp_extract(col('rating'), r'(\d+\.\d+)', 0).cast('float'))

    # Define base parquet file path in S3 bucket
    base_parquet_dir = 'dbfs:/mnt/bestpricenphu/data/processed/amazon'

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define new parquet file path in S3 bucket
    parquet_file = f'{base_parquet_dir}/amazon_data_{model_formatted}_{timestamp}.parquet'

    # Saving the processed data
    df.write.mode('overwrite').parquet(parquet_file)

model = 'bestpricenphu'  # Replace 'model_name' with your model name
process_amazon_data_main(model)

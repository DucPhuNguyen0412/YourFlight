from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark Session
spark = SparkSession.builder \
    .appName('Amazon Data Processing') \
    .getOrCreate()

# Read the CSV data
df = spark.read.csv('../../data/raw/amazon_data.csv', header=True, inferSchema=True)

# Filter the DataFrame for the model "playstation 4"
df = df.filter(col('model') == 'playstation 4')

# Perform data cleaning and processing here...

# For example, removing null values:
df = df.dropna()

# You could perform some transformations, like extracting numeric value from 'rating'
df = df.withColumn('rating', col('rating').substr(1, 3).cast('float'))

# Saving the processed data
df.write.parquet('../../data/processed/amazon_data.parquet')

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("QueryParquet").getOrCreate()
    
    df = spark.read.parquet('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/amazon_data.parquet/part-00000-79151c56-d99d-4d11-9511-7ff6b09c0d0f-c000.snappy.parquet')
    df.createOrReplaceTempView("view")
    
    # Check schema
    print("Dataframe schema:")
    df.printSchema()
    
    # Check all records
    print("All records:")
    all_records_df = spark.sql("SELECT * FROM view")
    all_records_df.show()
    
    # Check the count of records where the model is 'playstation 4'
    print("Count of records where model = 'playstation 4':")
    count_df = spark.sql("SELECT COUNT(*) FROM view WHERE model = 'playstation 4'")
    count_df.show()
    
    # Example query
    result_df = spark.sql("SELECT * FROM view WHERE model = 'playstation 4'")
    
    # Show the result in the console
    print("Query result:")
    result_df.show()

if __name__ == "__main__":
    main()

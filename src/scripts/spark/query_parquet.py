from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("QueryParquet").getOrCreate()

    # Read parquet file path from text file
    with open('/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/parquet_file_path.txt', 'r') as f:
        parquet_file = f.read().strip()

    # Read parquet file
    df = spark.read.parquet(parquet_file)

    df.createOrReplaceTempView("view")

    # # Check all records
    # print("All records:")
    # all_records_df = spark.sql("SELECT * FROM view")
    # all_records_df.show()

    # Get the distinct model values from the dataframe
    distinct_models = df.select("model").distinct().rdd.flatMap(lambda x: x).collect()

    # Query for each distinct model
    for model in distinct_models:
        query = f"SELECT * FROM view WHERE model = '{model}'"
        result_df = spark.sql(query)

        # Show the query result for each model
        print(f"Query result for model '{model}':")
        result_df.show()

if __name__ == "__main__":
    main()

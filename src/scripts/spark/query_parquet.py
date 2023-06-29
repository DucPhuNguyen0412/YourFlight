from pyspark.sql import SparkSession
import os
import sys

def main(model):
    spark = SparkSession.builder.appName("QueryParquet").getOrCreate()

    # Format model string for use in file paths
    model_formatted = model.replace(' ', '_')

    # Define base parquet file paths
    base_parquet_dir_amazon = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/amazon/'
    base_parquet_dir_ebay = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/processed/ebay/'

    # Find the most recent Parquet file for this model in Amazon data
    if os.path.exists(base_parquet_dir_amazon):
        model_parquet_files_amazon = [file for file in os.listdir(base_parquet_dir_amazon) if f'amazon_data_{model_formatted}_' in file]
        model_parquet_files_amazon.sort(reverse=True)
        parquet_file_amazon = os.path.join(base_parquet_dir_amazon, model_parquet_files_amazon[0])

        # Read parquet files
        df_amazon = spark.read.parquet(parquet_file_amazon)

        # Create temporary views
        df_amazon.createOrReplaceTempView("view_amazon")

        # Query for this model in Amazon data
        query_amazon = f"SELECT model, title, price, rating, reviews FROM view_amazon WHERE model = '{model}'"
        result_df_amazon = spark.sql(query_amazon)

        # Show the query results for this model from Amazon
        print(f"Amazon query result for model '{model}':")
        result_df_amazon.show(40)
    else:
        print(f"Could not find directory: {base_parquet_dir_amazon}")

    # Find the most recent Parquet file for this model in eBay data
    if os.path.exists(base_parquet_dir_ebay):
        model_parquet_files_ebay = [file for file in os.listdir(base_parquet_dir_ebay) if f'ebay_data_{model_formatted}_' in file]
        model_parquet_files_ebay.sort(reverse=True)
        parquet_file_ebay = os.path.join(base_parquet_dir_ebay, model_parquet_files_ebay[0])

        # Read parquet files
        df_ebay = spark.read.parquet(parquet_file_ebay)

        # Create temporary views
        df_ebay.createOrReplaceTempView("view_ebay")

        # Query for this model in eBay data
        query_ebay = f"SELECT model, title, price, rating, reviews FROM view_ebay WHERE model = '{model}'"
        result_df_ebay = spark.sql(query_ebay)

        # Show the query results for this model from eBay
        print(f"eBay query result for model '{model}':")
        result_df_ebay.show(40)
    else:
        print(f"Could not find directory: {base_parquet_dir_ebay}")

if __name__ == "__main__":
    model = sys.argv[1]  # Get model name from command line argument
    main(model)

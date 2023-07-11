#!/bin/bash

# Ask for input
echo "Enter the models to search for (separated by commas): "
read models

# Split models by comma into an array
IFS=',' read -ra model_array <<< "$models"

# Pass each model to the Python scripts
for model in "${model_array[@]}"; do
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/amazon_web_scraping.py "$model" >> script.log 2>&1
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/ebay_web_scraping.py "$model" >> script.log 2>&1
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/spark/process_amazon_data.py "$model" >> script.log 2>&1
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/spark/process_ebay_data.py "$model" >> script.log 2>&1
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/spark/query_parquet.py "$model" >> script.log 2>&1
done

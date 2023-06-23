#!/bin/bash

# Ask for input
echo "Enter the models to search for (separated by commas): "
read models

# Pass the models string directly to the Python script
python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/amazon_web_scraping.py "$models"
python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/spark/process_amazon_data.py
python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/spark/query_parquet.py

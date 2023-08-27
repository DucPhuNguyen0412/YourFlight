#!/bin/bash

# Bucket name
bucket="bestpricenphu"

# Databricks host and token
databricks_host="https://dbc-9fd8929d-2da0.cloud.databricks.com"
databricks_token="dapi1759551c1da9454b0a991e47f04ec161"

# Ask for input
echo "Enter the models to search for (separated by commas): "
read models

# Split models by comma into an array
IFS=',' read -ra model_array <<< "$models"

# Pass each model and bucket to the Python scripts and Databricks notebooks
for model in "${model_array[@]}"; do
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/amazon_web_scraping.py "$model" "$bucket" >> script.log 2>&1
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/ebay_web_scraping.py "$model" "$bucket" >> script.log 2>&1

    # Run the Databricks notebooks
    databricks jobs submit --profile default \
    --access-token $databricks_token \
    --workspace-url $databricks_host \
    /Users/nphu01@vt.edu/process_amazon_data \
    --parameters "{\"model\": \"$model\", \"bucket\": \"$bucket\"}" >> script.log 2>&1

    databricks jobs submit --profile default \
        --access-token $databricks_token \
        --workspace-url $databricks_host \
        /Users/nphu01@vt.edu/process_ebay_data \
        --parameters "{\"model\": \"$model\", \"bucket\": \"$bucket\"}" >> script.log 2>&1

    databricks workspace execute --profile default \
        --access-token $databricks_token \
        --workspace-url $databricks_host \
        /Users/nphu01@vt.edu/query_parquets \
        --parameters "{\"model\": \"$model\", \"bucket\": \"$bucket\"}" >> script.log 2>&1
done

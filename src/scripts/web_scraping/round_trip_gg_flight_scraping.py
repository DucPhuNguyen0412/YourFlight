from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import boto3
from datetime import datetime, timedelta
import pandas as pd

origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")
return_date = input("Enter the return date (YYYY-MM-DD): ")
bucket_name = "bestpricenphu"

def push_to_s3(bucket, data, date, return_date):
    session = boto3.Session()
    s3 = session.client('s3')

    # convert scraped data to JSON
    json_data = json.dumps(data, indent=4)

    # create the s3 key
    current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    json_key = f'data/raw/google-flights/round-/{current_time}_flight_{origin}_{destination}_{date}_{return_date}_raw_data.json'

    # write data to S3
    s3.put_object(Bucket=bucket, Body=json_data, Key=json_key)

# Calculate the date ranges
date_input = pd.to_datetime(date)
start_date = date_input - timedelta(days=3)
end_date = date_input + timedelta(days=3)

return_date_input = pd.to_datetime(return_date)
start_return_date = return_date_input - timedelta(days=3)
end_return_date = return_date_input + timedelta(days=3)

# Create date ranges
departure_dates = pd.date_range(start=start_date, end=end_date).tolist()
return_dates = pd.date_range(start=start_return_date, end=end_return_date).tolist()

driver = webdriver.Chrome()

for date in departure_dates:
    for return_date in return_dates:
        date_str = date.strftime('%Y-%m-%d')
        return_date_str = return_date.strftime('%Y-%m-%d')

        url = f"https://www.google.com/flights?hl=en#flt={origin}.{destination}.{date_str}*{destination}.{origin}.{return_date_str}"

        driver.get(url)

        # Add explicit wait here
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, 'YOUR XPATH FOR FLIGHT LISTINGS HERE')))

        flight_rows = driver.find_elements(By.XPATH, 'YOUR XPATH FOR FLIGHT LISTINGS HERE')

        flights_data = []

        for flight_row in flight_rows:
            flight_data = {}

            # Extract the details (like airlines, departure and arrival times, duration, stops, etc.)
            flights_data.append(flight_data)

        push_to_s3(bucket_name, flights_data, date_str, return_date_str)

driver.quit()

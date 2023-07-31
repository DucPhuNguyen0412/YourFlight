from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import boto3
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures

origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")
bucket_name = "bestpricenphu"

def push_to_s3(bucket, data):
    session = boto3.Session()
    s3 = session.client('s3')

    # convert scraped data to JSON
    json_data = json.dumps(data, indent=4)

    # create the s3 key
    current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    json_key = f'data/raw/kayak/one-way/{current_time}_flight_{origin}_{destination}_{date}_raw_data.json'

    # write data to S3
    s3.put_object(Bucket=bucket, Body=json_data, Key=json_key)

def scrape_date(date):
    driver = webdriver.Chrome()
    date_str = date.strftime('%Y-%m-%d')  # Convert datetime object to string

    url = f"https://www.kayak.com/flights/{origin}-{destination}/{date_str}?sort=price_a"

    driver.get(url)
    sleep(10)

    flight_rows = driver.find_elements(By.XPATH,'//div[@class="nrc6-inner"]')

    flights_data = []

    for element in flight_rows:
        # Initialize an empty dictionary to hold flight data
        flight_data = {}

        # Price
        price = element.find_element(By.CSS_SELECTOR, "div.nrc6-price-section div.f8F1-price-text-container").text
        flight_data["price"] = price

        # Company names
        company_names = element.find_element(By.CSS_SELECTOR, "div.c_cgF.c_cgF-mod-variant-default").text
        flight_data["company_names"] = company_names

        # Start and end times
        times = element.find_element(By.CSS_SELECTOR, "div.vmXl.vmXl-mod-variant-large").text.replace("\u2013", "-")
        times = times.replace(" am", "am ").replace(" pm", "pm ").replace("-", "- ")
        flight_data["start_end_time"] = times

        # Duration
        duration = element.find_element(By.CSS_SELECTOR, "div.xdW8 .vmXl.vmXl-mod-variant-default").text.replace("\u2013", "-")
        flight_data["duration"] = duration

        # Number of stops
        num_stops = element.find_element(By.CSS_SELECTOR, "div.vmXl.vmXl-mod-variant-default").text
        flight_data["num_stops"] = num_stops

        # Stop destinations
        stop_destinations = element.find_element(By.CSS_SELECTOR, "div.JWEO div.c_cgF.c_cgF-mod-variant-default").text
        flight_data["stop_destinations"] = stop_destinations

        # Add the dictionary to the flights_data list
        flights_data.append(flight_data)

    # Now push the collected data to S3 bucket
    push_to_s3(bucket_name, flights_data)

    driver.quit()

# Calculate the date range
date_input = pd.to_datetime(date)  # Convert the input date to datetime object
start_date = date_input - timedelta(days=3)  # 3 days before the given date
end_date = date_input + timedelta(days=3)  # 3 days after the given date

# Create date range
dates = pd.date_range(start=start_date, end=end_date).tolist()

# Use ThreadPoolExecutor to run the tasks concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(scrape_date, dates)
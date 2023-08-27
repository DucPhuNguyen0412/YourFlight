from selenium import webdriver
from selenium.webdriver.common.by import By
import json
from time import sleep
import boto3
from datetime import datetime, timedelta
import pandas as pd
import concurrent.futures

origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")
bucket_name = "bestpricenphu"

def push_to_s3(bucket, data, date_str):
    session = boto3.Session()
    s3 = session.client('s3')

    # Convert scraped data to JSON
    json_data = json.dumps(data, indent=4)

    # Create the s3 key
    current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    json_key = f'data/raw/google-flights/one-way/{current_time}_flight_{origin}_{destination}_{date_str}_raw_data.json'

    # Write data to S3
    s3.put_object(Bucket=bucket, Body=json_data, Key=json_key)

def scrape_date(date):
    driver = webdriver.Chrome()
    date_str = date.strftime('%Y-%m-%d')

    # Navigate to Google Flights with the updated URL format
    url = f'https://www.google.com/travel/flights?hl=en&q=Flights%20to%20{destination}%20from%20{origin}%20on%20{date_str}%20oneway'
    driver.get(url)

    sleep(10)  # Wait for the page to load

    flights = driver.find_elements(By.XPATH, '//li[@data-test-id="offer-list"]')
    flight_details = []

    for flight in flights:
        details = {}
        try:
            # Extracting the airline and the 'Operated by' information
            airline_info = flight.find_element(By.XPATH, './/span[@class="h1fkLb"]').text
            if "Operated by" in airline_info:
                details['airline'] = ' '.join(airline_info.split()[2:])
            else:
                details['airline'] = airline_info

            # New XPaths for arrival_time, departure_time, and price
            details['price'] = flight.find_element(By.XPATH, './/div[contains(@class, "YMlIz FpEdX jLMuyc")]/span').text
            details['departure_time'] = flight.find_element(By.XPATH, './/div[contains(@aria-label, "Departure time:")]').text
            details['arrival_time'] = flight.find_element(By.XPATH, './/div[contains(@aria-label, "Arrival time:")]').text
            duration_element = flight.find_element(By.XPATH, './/span[contains(text(), "hr")]')
            details['duration'] = duration_element.text
            flight_details.append(details)
        except Exception as e:
            print(f"Error extracting flight info: {e}")

    driver.quit()

    # Push the collected data to S3 bucket
    push_to_s3(bucket_name, flight_details, date_str)

# Calculate the date range
date_input = pd.to_datetime(date)
start_date = date_input - timedelta(days=3)
end_date = date_input + timedelta(days=3)

dates = pd.date_range(start=start_date, end=end_date).tolist()

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(scrape_date, dates)

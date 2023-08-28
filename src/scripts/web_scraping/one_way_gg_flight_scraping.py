from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from time import sleep
import boto3
from datetime import datetime, timedelta
import pandas as pd

origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")
bucket_name = "bestpricenphu"

def push_to_s3(bucket, data, date_str):
    print("Pushing data to S3...")
    session = boto3.Session()
    s3 = session.client('s3')
    json_data = json.dumps(data, indent=4)
    current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    json_key = f'data/raw/google-flights/one-way/{current_time}_flight_{origin}_{destination}_{date_str}_raw_data.json'
    s3.put_object(Bucket=bucket, Body=json_data, Key=json_key)
    print("Data pushed to S3 successfully!")

def scrape_date(date):
    print(f"Initializing Chrome driver for {date}...")
    driver = webdriver.Chrome()
    date_str = date.strftime('%Y-%m-%d')
    url = f'https://www.google.com/travel/flights?hl=en&q=Flights%20to%20{destination}%20from%20{origin}%20on%20{date_str}%20oneway'
    print(f"Accessing URL: {url}")
    driver.get(url)

    # Using WebDriverWait to ensure the flights' data is loaded
    print("Waiting for flights' data to be loaded...")
    wait = WebDriverWait(driver, 60)
    wait.until(EC.presence_of_element_located((By.XPATH, '//li[@data-test-id="offer-list"]')))
    print("Flights' data loaded!")

    flights = driver.find_elements(By.XPATH, '//li[@data-test-id="offer-list"]')
    flight_details = []

    print(f"Extracting details from {len(flights)} flights...")
    for flight in flights:
        details = {}
        try:
            # Extracting airline information
            airline_info = flight.find_element(By.XPATH, './/span[@class="h1fkLb"]/span').text
            if "Operated by" in airline_info:
                details['airline'] = ' '.join(airline_info.split()[2:])
            else:
                details['airline'] = airline_info
            
            # Extracting price, departure time, arrival time, and duration
            details['price'] = flight.find_element(By.XPATH, './/div[contains(@class, "YMlIz FpEdX")]/span').text
            details['departure_time'] = flight.find_element(By.XPATH, './/div[contains(@aria-label, "Departure time:")]').text
            details['arrival_time'] = flight.find_element(By.XPATH, './/div[contains(@aria-label, "Arrival time:")]').text
            duration_element = flight.find_element(By.XPATH, './/div[contains(@class, "YMlIz FpEdX")]/span/following-sibling::span')
            details['duration'] = duration_element.text

            flight_details.append(details)
        except Exception as e:
            print(f"Error extracting flight info: {e}")

    print(f"Scraped Data for {date_str}:")
    print(flight_details)

    driver.quit()
    push_to_s3(bucket_name, flight_details, date_str)
    
date_input = pd.to_datetime(date)
start_date = date_input - timedelta(days=3)
end_date = date_input + timedelta(days=3)
dates = pd.date_range(start=start_date, end=end_date).tolist()

print(f"Scraping data for {len(dates)} days...")
# Loop through each date and scrape sequentially
for date in dates:
    try:
        scrape_date(date)
    except Exception as e:
        print(f"Error on {date.strftime('%Y-%m-%d')}: {e}")

print("Scraping process completed!")

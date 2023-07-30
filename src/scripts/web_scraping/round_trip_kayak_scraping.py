from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
import json
import boto3
from datetime import datetime

origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")
return_date = input("Enter the return date (YYYY-MM-DD): ")
bucket_name = "bestpricenphu"

def push_to_s3(bucket, data):
    session = boto3.Session()
    s3 = session.client('s3')

   # convert scraped data to JSON
    json_data = json.dumps(data, indent=4)

    # create the s3 key
    current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    json_key = f'data/raw/kayak/round-way/{current_time}_flight_{origin}_{destination}_{date}_{return_date}_raw_data.json'

    # write data to S3
    s3.put_object(Bucket=bucket, Body=json_data, Key=json_key)

driver = webdriver.Chrome()

url = f"https://www.kayak.com/flights/{origin}-{destination}/{date}/{return_date}?sort=price_a"

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

    flight_segments = element.find_elements(By.XPATH, './/li[@class="hJSA-item"]')

    flight_data["flights"] = []
    for segment in flight_segments:
        # Initialize an empty dictionary for this segment
        segment_data = {}

        # Company names
        company_names = segment.find_element(By.CSS_SELECTOR, "div.c_cgF.c_cgF-mod-variant-default").text
        segment_data["company_names"] = company_names

        # Start and end times
        times = segment.find_element(By.CSS_SELECTOR, "div.vmXl.vmXl-mod-variant-large").text.replace("\u2013", "-")
        times = times.replace(" am", "am ").replace(" pm", "pm ").replace("-", "- ")
        segment_data["start_end_time"] = times

        # Duration
        duration = segment.find_element(By.CSS_SELECTOR, "div.xdW8 .vmXl.vmXl-mod-variant-default").text.replace("\u2013", "-")
        segment_data["duration"] = duration

        # Number of stops
        num_stops = segment.find_element(By.CSS_SELECTOR, "div.vmXl.vmXl-mod-variant-default").text
        segment_data["num_stops"] = num_stops

        # Stop destinations
        stop_destinations = segment.find_element(By.CSS_SELECTOR, "div.JWEO div.c_cgF.c_cgF-mod-variant-default").text
        segment_data["stop_destinations"] = stop_destinations

        # Add this segment's data to the flight data
        flight_data["flights"].append(segment_data)

    # Add the dictionary to the flights_data list
    flights_data.append(flight_data)

driver.quit()  # Don't forget to quit the driver at the end of the script

# Now push the collected data to S3 bucket
push_to_s3(bucket_name, flights_data)

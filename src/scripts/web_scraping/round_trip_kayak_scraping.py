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
    json_key = f'data/raw/kayak/round-/{current_time}_flight_{origin}_{destination}_{date}_{return_date}_raw_data.json'

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

        url = f"https://www.kayak.com/flights/{origin}-{destination}/{date_str}/{return_date_str}?sort=price_a"

        driver.get(url)

        # Add explicit wait here
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@class="nrc6-inner"]')))

        flight_rows = driver.find_elements(By.XPATH,'//div[@class="nrc6-inner"]')

        for i in range(len(flight_rows)):
            flight_data = {}
            flight_row = driver.find_element(By.XPATH, f'//div[@class="nrc6-inner"][{i+1}]')
            flight_segments = flight_row.find_elements(By.XPATH, './/li[@class="hJSA-item"]')

            flight_data["flights"] = []
            for j in range(len(flight_segments)):
                segment = driver.find_element(By.XPATH, f'//div[@class="nrc6-inner"][{i+1}]//li[@class="hJSA-item"][{j+1}]')
                segment_data = {}

                try:
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
                    try:
                        stop_destinations = segment.find_element(By.CSS_SELECTOR, "div.JWEO div.c_cgF.c_cgF-mod-variant-default").text
                        segment_data["stop_destinations"] = stop_destinations
                    except Exception as e:
                        print(f"An exception occurred: {e}")
                        segment_data["stop_destinations"] = "Non-stop"

                except Exception as e:
                    print(f"An exception occurred: {e}")

                flight_data["flights"].append(segment_data)

            push_to_s3(bucket_name, flight_data, date_str, return_date_str)

driver.quit()  # Don't forget to quit the driver at the end of the script
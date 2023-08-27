from selenium import webdriver
from selenium.webdriver.common.by import By
import json
from time import sleep

# Input parameters
origin = input("Enter the origin: ")
destination = input("Enter the destination: ")
date = input("Enter the departure date (YYYY-MM-DD): ")

def scrape_flights(origin, destination, date):
    # Open a new browser instance
    driver = webdriver.Chrome()

    # Navigate to Google Flights with the required parameters
    url = f"https://www.google.com/flights?hl=en#flt={origin}.{destination}.{date}"
    driver.get(url)

    # Wait for the page to load
    sleep(10)

    # Extract flight details
    flights = driver.find_elements(By.XPATH, '//li[@data-test-id="offer-list"]')

    flight_details = []
    for flight in flights:
        details = {}

        try:
            # Extracting data for each flight
            details['airline'] = flight.find_element(By.XPATH, './/span[@data-test-id="airline-name"]').text
            details['price'] = flight.find_element(By.XPATH, './/span[@data-test-id="price-text"]').text
            details['departure_time'] = flight.find_element(By.XPATH, './/span[@data-test-id="departure-time"]').text
            details['arrival_time'] = flight.find_element(By.XPATH, './/span[@data-test-id="arrival-time"]').text
            details['duration'] = flight.find_element(By.XPATH, './/span[@data-test-id="duration"]').text

            # Add to list
            flight_details.append(details)
        except Exception as e:
            print(f"Error extracting flight info: {e}")

    # Close the browser
    driver.quit()

    # Convert flight details to JSON format and save
    with open('flights.json', 'w') as f:
        json.dump(flight_details, f, indent=4)

    print("Scraping completed!")

scrape_flights(origin, destination, date)

import argparse
from bs4 import BeautifulSoup
import requests
import csv
from itertools import cycle
from get_proxies import get_proxies
from fake_useragent import UserAgent
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime

# Function to extract Product Title from Search Result
def get_search_title(item):
    try:
        title = item.find("span", class_="a-size-base-plus a-color-base a-text-normal")
        if title is None:  # If the first span class isn't found, try the second one
            title = item.find("span", class_="a-size-medium a-color-base a-text-normal")
        return title.text.strip()
    except AttributeError:
        return ""

# Function to extract Product Price from Search Result
def get_search_price(item):
    try:
        price = item.find("span", attrs={'class':'a-offscreen'}).text.strip()
    except AttributeError:
        price = ""
    return price

# Function to extract Product Rating from Search Result
def get_search_rating(item):
    try:
        rating = item.find("span", class_="a-icon-alt").text.strip()
    except AttributeError:
        rating = ""
    return rating

# Function to extract Number of User Reviews from Search Result
def get_search_review_count(item):
    try:
        review_count = item.find("span", class_="a-size-base s-underline-text").text.strip()
    except AttributeError:
        review_count = ""
    return review_count


def fetch_data_for_model(model, proxies):
    ua = UserAgent()
    proxy_pool = cycle(proxies)
    start_time = datetime.now()

    data = []
    for _ in range(len(proxies)):  # To ensure we don't enter an infinite loop if all proxies fail
        start_time1 = datetime.now()
        proxy = next(proxy_pool)
        headers = {'User-Agent': ua.random, 'Accept-Language': 'en-US, en;q=0.5'}
        url = f"https://www.amazon.com/s?k={model}"

        try:
            response = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy})
            response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
        except RequestException:
            continue  # Try next proxy

        soup = BeautifulSoup(response.content, "html.parser")
        items = soup.find_all("div", class_='a-section a-spacing-base')

        for item in items:
            start_time2 = datetime.now()
            title = get_search_title(item)
            price = get_search_price(item)
            rating = get_search_rating(item)
            review_count = get_search_review_count(item)

            data.append([model.replace("+", " "), title, price, rating, review_count])
            finish_time2 = datetime.now()
            duration2 = finish_time2 - start_time2
            print(f'for2 - duration {duration2}')
            #print(f"Fetched data for model: {model}")
        finish_time1 = datetime.now()
        duration1 = finish_time1 - start_time1
        print(f'for1 - duration {duration1}')
        break  # Successfully scraped this model, move on to the next one
    finish_time = datetime.now()
    duration = finish_time - start_time
    print(f'fetch_model - duration {duration}')
    
    print(f"Completed fetching data for model: {model}")
    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Amazon for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    args = parser.parse_args()

    # Split models by comma and remove leading/trailing whitespace
    models = [model.strip() for model in args.models.split(',')]
    
    proxies = get_proxies()  # Fetch proxies

    all_data = []
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        results = executor.map(fetch_data_for_model, models, [proxies]*len(models))
        for result in results:
            all_data.extend(result)
    
    with open("/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/amazon_data.csv", "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["model", "title", "price", "rating", "reviews"])
        writer.writerows(all_data)

    print("All data fetching completed")

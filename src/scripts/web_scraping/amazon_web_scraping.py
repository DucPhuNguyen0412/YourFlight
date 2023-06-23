import argparse
from bs4 import BeautifulSoup
import requests
import csv
from itertools import cycle
from get_proxies import get_proxies
import time
from fake_useragent import UserAgent
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor

# Function to extract Product Title
def get_title(soup):
    try:
        title = soup.find("span", attrs={"id":'productTitle'})
        title_value = title.string
        title_string = title_value.strip()
    except AttributeError:
        title_string = ""
    return title_string

# Function to extract Product Price
def get_price(soup):
    try:
        price = soup.find("span", attrs={'class': 'a-price'})
        price_value = price.find("span", attrs={'class': 'a-offscreen'}).text.strip()
    except AttributeError:
        price_value = ""
    return price_value

# Function to extract Product Rating
def get_rating(soup):
    try:
        rating = soup.find("i", attrs={'class':'a-icon a-icon-star a-star-4-5'}).string.strip()
    except AttributeError:
        try:
            rating = soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except:
            rating = ""
    return rating

# Function to extract Number of User Reviews
def get_review_count(soup):
    try:
        review_count = soup.find("span", attrs={'id':'acrCustomerReviewText'}).string.strip()
    except AttributeError:
        review_count = ""
    return review_count

# Function to extract Availability Status
def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id':'availability'})
        available = available.find("span").string.strip()
    except AttributeError:
        available = "Not Available"
    return available

# Cache dictionary
cache = {}

def fetch_data_for_model(model, proxies):
    ua = UserAgent()
    proxy_pool = cycle(proxies)

    if model in cache:
        return cache[model]

    data = []
    for _ in range(len(proxies)):  # To ensure we don't enter an infinite loop if all proxies fail
        proxy = next(proxy_pool)
        headers = {'User-Agent': ua.random, 'Accept-Language': 'en-US, en;q=0.5'}
        url = f"https://www.amazon.com/s?k={model}&ref=nb_sb_noss_2"

        try:
            response = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy})
            response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
        except RequestException:
            continue  # Try next proxy

        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", attrs={'class':'a-link-normal s-no-outline'})
        links_list = [link.get('href') for link in links]

        for link in links_list:
            try:
                if link.startswith('https'):
                    new_response = requests.get(link, headers=headers, proxies={'http': proxy, 'https': proxy})
                else:
                    new_response = requests.get("https://www.amazon.com" + link, headers=headers, proxies={'http': proxy, 'https': proxy})
                new_response.raise_for_status()
            except RequestException:
                continue  # Skip this product and try next one

            new_soup = BeautifulSoup(new_response.content, "html.parser")
            data.append([model.replace("+", " "), get_title(new_soup), get_price(new_soup), get_rating(new_soup), get_review_count(new_soup), get_availability(new_soup)])
            print(f"Fetched data for model: {model}, Link: {link}")
            
        cache[model] = data
        break  # Successfully scraped this model, move on to the next one
    
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
        writer.writerow(["model", "title", "price", "rating", "reviews", "availability"])
        writer.writerows(all_data)

    print("All data fetching completed")

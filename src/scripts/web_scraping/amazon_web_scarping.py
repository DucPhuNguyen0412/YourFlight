from bs4 import BeautifulSoup
import requests
import csv

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

def amazon_scrape_main(models):
    HEADERS = ({'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15', 'Accept-Language': 'en-US, en;q=0.5'})
    data = []

    for model in models:
        URL = f"https://www.amazon.com/s?k={model}&ref=nb_sb_noss_2"
        webpage = requests.get(URL, headers=HEADERS)
        soup = BeautifulSoup(webpage.content, "html.parser")
        links = soup.find_all("a", attrs={'class':'a-link-normal s-no-outline'})
        links_list = [link.get('href') for link in links]

        for link in links_list:
            if link.startswith('https'):
                new_webpage = requests.get(link, headers=HEADERS)
            else:
                new_webpage = requests.get("https://www.amazon.com" + link, headers=HEADERS)
            new_soup = BeautifulSoup(new_webpage.content, "html.parser")
            data.append([model.replace("+", " "), get_title(new_soup), get_price(new_soup), get_rating(new_soup), get_review_count(new_soup), get_availability(new_soup)])

    return data

if __name__ == "__main__":
    # Get user input for models
    models_input = input("Enter the models to search for (separated by commas): ")
    models = models_input.split(",")
    data = amazon_scrape_main(models)
    with open("/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/amazon_data.csv", "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["model", "title", "price", "rating", "reviews", "availability"])
        writer.writerows(data)

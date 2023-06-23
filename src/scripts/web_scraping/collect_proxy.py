import requests
import csv
from bs4 import BeautifulSoup
import os
from fake_useragent import UserAgent

def scrape_proxies():
    proxies = []
    url = 'https://free-proxy-list.net/'
    headers = {
        'User-Agent': UserAgent().random,
        'Accept-Language': 'en-US, en;q=0.5'
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')

    for row in table.tbody.find_all('tr'):
        columns = row.find_all('td')
        if columns[4].get_text() == 'elite proxy' and columns[6].get_text() == 'yes':
            ip = columns[0].get_text()
            port = columns[1].get_text()
            proxies.append(f"{ip}:{port}")
    
    return proxies

def save_proxies(proxies, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['proxy'])
        writer.writerows([[proxy] for proxy in proxies])

def main():
    proxies = scrape_proxies()
    path = "/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw"
    filename = os.path.join(path, 'proxy_list.csv')
    save_proxies(proxies, filename)
    print("Proxies saved successfully!")

if __name__ == "__main__":
    main()

import csv
import requests

def check_proxy(proxy):
    try:
        response = requests.get("https://www.google.com", proxies={"http": proxy, "https": proxy}, timeout=5)
        if response.status_code == 200:
            return True
    except requests.exceptions.RequestException:
        pass
    return False

def get_proxies():
    proxies = []
    with open("/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/proxy_list.csv", "r") as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip the headers
        for row in reader:
            proxy = row[0]
            if check_proxy(proxy):
                proxies.append(proxy)
    return proxies

if __name__ == "__main__":
    get_proxies()

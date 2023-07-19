import argparse
import os
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
import boto3
import pandas as pd
from io import StringIO
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import glob

def delete_old_files(bucket_name, folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=folder):
        s3.Object(bucket_name, obj.key).delete()

# Setup logging
log_file = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/log/kayak_scrapy.log'
if os.path.exists(log_file):
    os.remove(log_file)
logging.basicConfig(filename=log_file, level=logging.INFO)

session = boto3.Session()
s3 = session.client('s3')

class FlightItem(scrapy.Item):
    price = scrapy.Field()
    airline = scrapy.Field()
    label = scrapy.Field()
    flight_info = scrapy.Field()
    operated_by = scrapy.Field()
    class_type = scrapy.Field()
    carry_on_bag = scrapy.Field()
    checked_bag = scrapy.Field()

class KayakSpider(scrapy.Spider):
    name = "kayak_spider"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'

    def __init__(self, departure, destination, depart_date, return_date, travelers, cabin_class, flight_type, bucket):
        self.departure = departure
        self.destination = destination
        self.depart_date = depart_date
        self.return_date = return_date
        self.travelers = travelers
        self.cabin_class = cabin_class
        self.flight_type = flight_type
        self.bucket = bucket
        super(KayakSpider, self).__init__()
        self.start_urls = [f"https://www.kayak.com/flights/{self.departure}-{self.destination}/{self.depart_date}/{self.return_date}?travelers={self.travelers}&cabin={self.cabin_class}&flight_type={self.flight_type}"]
        self.items = []
        options = Options()
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def start_requests(self):
        for url in self.start_urls:
            self.driver.get(url)
            print("Please select the type of flight: 1(Round-trip), 2(One-way), 3(Multi-city)")
            flight_type = input("Enter your choice: ")

            # Map user input to HTML id
            flight_type_dict = {"1": "roundtrip", "2": "oneway", "3": "multicity"}
            flight_type_id = flight_type_dict.get(flight_type, "roundtrip")

            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.ID, flight_type_id))).click()

            # Rendered HTML after interaction
            html = self.driver.page_source

            # Scrapy request with updated HTML
            yield scrapy.Request(url, callback=self.parse, headers={'User-Agent': self.user_agent}, body=html, method='GET')

    def parse(self, response):
        for row in response.css('div.resultInner'):
            l = ItemLoader(item=FlightItem(), selector=row)
            l.add_css('price', 'div.f8F1-price-text::text')
            l.add_css('airline', 'div.VY2U > div.c_cgF::text')
            l.add_css('label', 'div.btf6-badge-wrap > div::text')
            l.add_css('flight_info', 'div.hJSA::text')
            l.add_css('operated_by', 'div.J0g6-operator-text::text')
            l.add_css('class_type', 'div.aC3z-name::text')
            l.add_css('carry_on_bag', 'div.ac27-inner::text')
            l.add_css('checked_bag', 'div.ac27-inner::text')
            self.items.append(l.load_item())

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KayakSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        # convert scraped data to pandas dataframe
        df = pd.DataFrame(self.items)

        # create a buffer and write data to it
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        # create the s3 key
        current_date = datetime.now().strftime('%Y-%m-%d')
        csv_key = f'data/raw/kayak/{current_date}_kayak_raw_data.csv'

        # write data to S3
        s3.put_object(Bucket=self.bucket, Body=csv_buffer.getvalue(), Key=csv_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Kayak for flight info")
    parser.add_argument("departure", help="Departure location")
    parser.add_argument("destination", help="Destination")
    parser.add_argument("depart_date", help="Departure date in format YYYY-MM-DD")
    parser.add_argument("return_date", help="Return date in format YYYY-MM-DD")
    parser.add_argument("travelers", help="Number of travelers")
    parser.add_argument("cabin_class", help="Cabin class (Economy, Premium Economy, Business, First)")
    parser.add_argument("flight_type", help="Type of flight (Round-Trip, One-way, Multi-city)")
    parser.add_argument("bucket", help="Bucket name on S3 to save results")

    args = parser.parse_args()

    # delete old files from the S3 bucket folder
    delete_old_files(args.bucket, 'data/raw/kayak')

    process = CrawlerProcess(settings={
        "FEEDS": {
            os.path.join(os.getcwd(), f"{KayakSpider.name}.csv"): {"format": "csv"},
        },
    })

    process.crawl(KayakSpider, args.departure, args.destination, args.depart_date, args.return_date, args.travelers, args.cabin_class, args.flight_type, args.bucket)
    process.start()  # the script will block here until the crawling is finished
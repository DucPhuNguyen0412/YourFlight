import argparse
import os
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import NoCredentialsError
import logging
from datetime import datetime

def delete_old_files(bucket_name, folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=folder):
        s3.Object(bucket_name, obj.key).delete()

# Setup logging
log_file = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/log/ebay_scrapy.log'
if os.path.exists(log_file):
    os.remove(log_file)
logging.basicConfig(filename=log_file, level=logging.INFO)

session = boto3.Session()
s3 = session.client('s3')

class EbayItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class EbaySpider(scrapy.Spider):
    name = "ebay_spider"
    max_pages = 5 # the maximum number of pages to scrape for each model

    # User-Agent setup
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'

    def __init__(self, models, bucket):
        # check if models is a list or a string
        if isinstance(models, list):
            self.models = models
        else:
            self.models = models.split(',')
        self.bucket = bucket
        super(EbaySpider, self).__init__()
        self.start_urls = [f"https://www.ebay.com/sch/i.html?_nkw={model}" for model in self.models]
        self.items = []
        self.page_number = 1 # Initialize page number

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': self.user_agent})

    def parse(self, response):
        for item in response.css('div.s-item__wrapper'):
            l = ItemLoader(item=EbayItem(), selector=item)
            l.add_value('model', response.css('input#gh-ac::attr(value)').get())
            l.add_css('title', 'div.s-item__title ::text')
            l.add_css('price', '.s-item__price::text')
            l.add_css('rating', 'div.x-star-rating span.clipped::text')
            l.add_css('reviews', 'span.s-item__reviews-count span[aria-hidden="false"]::text')

            self.items.append(l.load_item())

        # handling pagination
        next_page_url = response.css('a.pagination__next::attr(href)').get()
        if next_page_url and self.page_number < self.max_pages:
            self.page_number += 1
            yield scrapy.Request(response.urljoin(next_page_url), 
                                callback=self.parse, 
                                headers={'User-Agent': self.user_agent})

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EbaySpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        # convert scraped data to pandas dataframe
        df = pd.DataFrame(self.items)

        # create a buffer and write data to it
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        # create the s3 key
        current_date = datetime.now().strftime('%Y-%m-%d')
        csv_key = f'data/raw/ebay/{current_date}_ebay_raw_data.csv'

        # write data to S3
        s3.put_object(Bucket=self.bucket, Body=csv_buffer.getvalue(), Key=csv_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Ebay for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    parser.add_argument("bucket", help="AWS S3 bucket to store the scraped data")
    args = parser.parse_args()
    
    # delete old files from the S3 bucket folder
    delete_old_files(args.bucket, 'data/raw/ebay')

    process = CrawlerProcess({
        'USER_AGENT': EbaySpider.user_agent
    })

    models = [model.strip() for model in args.models.split(',')]
    process.crawl(EbaySpider, models=models, bucket=args.bucket)
    process.start()

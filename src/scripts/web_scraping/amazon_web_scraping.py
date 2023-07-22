import argparse
import os
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import boto3
import pandas as pd
from io import StringIO
import logging
from datetime import datetime

def delete_old_files(bucket_name, folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=folder):
        s3.Object(bucket_name, obj.key).delete()

# Setup logging
log_file = '/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/log/amazon_scrapy.log'
if os.path.exists(log_file):
    os.remove(log_file)
logging.basicConfig(filename=log_file, level=logging.INFO)

session = boto3.Session()
s3 = session.client('s3')

class AmazonItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class AmazonSpider(scrapy.Spider):
    name = "amazon_spider"
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
        super(AmazonSpider, self).__init__()
        self.start_urls = [f"https://www.amazon.com/s?k={model}" for model in self.models]
        self.items = []
        self.page_number = 1 # Initialize page number

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': self.user_agent})

    def parse(self, response):
        for item in response.css('div.a-section.a-spacing-base, div.a-section'):
            l = ItemLoader(item = AmazonItem(), selector=item)
            l.add_value('model', response.css('input#twotabsearchtextbox::attr(value)').get())
            l.add_css('title', 'span.a-size-base-plus.a-color-base.a-text-normal::text, span.a-size-medium.a-color-base.a-text-normal::text')
            l.add_css('price', 'span.a-price-whole::text, span.a-price-fraction::text')
            l.add_css('rating', 'i.a-icon.a-icon-star-small.a-star-small-4-5.aok-align-bottom span.a-icon-alt::text')
            l.add_css('reviews', 'a.a-link-normal.s-underline-text.s-underline-link-text.s-link-style span.a-size-base.s-underline-text::text')

            self.items.append(l.load_item())


        # handling pagination
        next_page_url = response.css('span.s-pagination-item.s-pagination-next a::attr(href)').get()
        if next_page_url and self.page_number < self.max_pages:
            self.page_number += 1
            yield scrapy.Request(response.urljoin(next_page_url), 
                                callback=self.parse, 
                                headers={'User-Agent': self.user_agent})

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonSpider, cls).from_crawler(crawler, *args, **kwargs)
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
        csv_key = f'data/raw/amazon/{current_date}_amazon_raw_data.csv'

        # write data to S3
        s3.put_object(Bucket=self.bucket, Body=csv_buffer.getvalue(), Key=csv_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Amazon for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    parser.add_argument("bucket", help="AWS S3 bucket to store the scraped data")
    args = parser.parse_args()

    # delete old files from the S3 bucket folder
    delete_old_files(args.bucket, 'data/raw/amazon')
    
    process = CrawlerProcess({
        'USER_AGENT': AmazonSpider.user_agent
    })

    models = [model.strip() for model in args.models.split(',')]
    process.crawl(AmazonSpider, models=models, bucket=args.bucket)
    process.start()

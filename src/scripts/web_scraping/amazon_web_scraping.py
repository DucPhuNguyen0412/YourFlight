import argparse
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import NoCredentialsError
import logging

# Setup logging
logging.basicConfig(filename='scrapy.log', level=logging.INFO)

# Setup AWS credentials
try:
    session = boto3.Session(profile_name='default')
    s3 = session.client('s3')
except NoCredentialsError:
    logging.error("Credentials not available")

class AmazonItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class AmazonSpider(scrapy.Spider):
    name = "amazon_spider"
    max_pages = 5 # the maximum number of pages to scrape for each model

    def __init__(self, models=None, bucket=None, *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.models = models.split(',')
        self.bucket = bucket
        self.start_urls = [f"https://www.amazon.com/s?k={model}" for model in self.models]
        self.items = []
        self.page_number = 1 # Initialize page number

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'})

    def parse(self, response):
        for item in response.css('div.a-section.a-spacing-base, div.a-section'):
            l = ItemLoader(item = AmazonItem(), selector=item)
            l.add_value('model', response.css('input#twotabsearchtextbox::attr(value)').get())
            l.add_css('title', 'span.a-size-base-plus.a-color-base.a-text-normal::text, span.a-size-medium.a-color-base.a-text-normal::text')
            l.add_css('price', 'span.a-offscreen::text')
            l.add_css('rating', 'span.a-icon-alt::text')
            l.add_css('reviews', 'span.a-size-base.s-underline-text::text')

            self.items.append(l.load_item())

        # handling pagination
        next_page_url = response.css('span.s-pagination-item.s-pagination-next a::attr(href)').get()
        if next_page_url and self.page_number < self.max_pages:
            self.page_number += 1
            yield scrapy.Request(response.urljoin(next_page_url), 
                                callback=self.parse, 
                                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'})

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
        csv_key = 'data/raw/amazon/amazon_raw_data.csv'

        # write data to S3
        s3.put_object(Bucket=self.bucket, Body=csv_buffer.getvalue(), Key=csv_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Amazon for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    parser.add_argument("bucket", help="AWS S3 bucket to store the scraped data")
    args = parser.parse_args()

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    })

    models = [model.strip() for model in args.models.split(',')]
    process.crawl(AmazonSpider, models=models, bucket=args.bucket)
    process.start()

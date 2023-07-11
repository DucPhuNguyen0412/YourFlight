import argparse
import scrapy
import boto3
import pandas as pd
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from io import StringIO
from botocore.exceptions import NoCredentialsError

# Setup AWS credentials
try:
    session = boto3.Session(profile_name='default')
    s3 = session.client('s3')
except NoCredentialsError:
    print("Credentials not available")

class EbayItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class EbaySpider(scrapy.Spider):
    name = "ebay_spider"
    max_pages = 5  # the maximum number of pages to scrape for each model

    def __init__(self, models=None, bucket=None, *args, **kwargs):
        super(EbaySpider, self).__init__(*args, **kwargs)
        self.models = models.split(',')
        self.bucket = bucket
        self.start_urls = [f"https://www.ebay.com/sch/i.html?_nkw={model}" for model in models]
        self.page_number = 1  # track the current page number

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'})

    def parse(self, response):
        for item in response.css('div.s-item__wrapper'):
            l = ItemLoader(item=EbayItem(), selector=item)
            l.add_value('model', response.css('input#gh-ac::attr(value)').get())
            l.add_css('title', 'div.s-item__title ::text')  # updated title selector
            l.add_css('price', '.s-item__price::text')
            l.add_css('rating', 'div.x-star-rating span.clipped::text')  # updated rating selector
            l.add_css('reviews', 'span.s-item__reviews-count span[aria-hidden="false"]::text')  # updated reviews selector

            yield l.load_item()

        # handling pagination
        next_page_url = response.css('a.pagination__next::attr(href)').get()
        if next_page_url and self.page_number < self.max_pages:
            self.page_number += 1
            yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'})
            
    def close(self, reason):
        # convert scraped data to pandas dataframe
        df = pd.DataFrame(self.items)

        # create a buffer and write data to it
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)

        # create the s3 key
        csv_key = 'data/raw/ebay/ebay_raw_data.csv'

        # write data to S3
        s3.put_object(Bucket=self.bucket, Body=csv_buffer.getvalue(), Key=csv_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape eBay for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    parser.add_argument("bucket", help="AWS S3 bucket to store the scraped data")
    args = parser.parse_args()

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
    })

    models = [model.strip() for model in args.models.split(',')]
    process.crawl(EbaySpider, models=models, bucket=args.bucket)
    process.start()
import argparse
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from fake_useragent import UserAgent

class AmazonItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class AmazonSpider(scrapy.Spider):
    name = "amazon_spider"
    max_pages = 5 # the maximum number of pages to scrape for each model

    def __init__(self, models=None, *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.start_urls = [f"https://www.amazon.com/s?k={model}" for model in models.split(',')]
        self.ua = UserAgent()

    def parse(self, response):
        for item in response.css('div.a-section.a-spacing-base'):
            l = ItemLoader(item = AmazonItem(), selector=item)
            l.add_value('model', response.css('input#twotabsearchtextbox::attr(value)').get())
            l.add_css('title', 'span.a-size-base-plus.a-color-base.a-text-normal::text')
            l.add_css('price', 'span.a-offscreen::text')
            l.add_css('rating', 'span.a-icon-alt::text')
            l.add_css('reviews', 'span.a-size-base.s-underline-text::text')

            yield l.load_item()

        # handling pagination
        next_page_url = response.css('span.s-pagination-item.s-pagination-next a::attr(href)').get()
        page_number = response.css('span.s-pagination-item.s-pagination-disabled::text').get()
        if next_page_url and int(page_number) <= self.max_pages:
            yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse, headers={'User-Agent': self.ua.random})

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': self.ua.random})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Amazon for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    args = parser.parse_args()

    process = CrawlerProcess({
        'USER_AGENT': UserAgent().random,
        'FEED_FORMAT': 'csv',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_proxy_pool.middlewares.ProxyPoolMiddleware': 610,
            'scrapy_proxy_pool.middlewares.BanDetectionMiddleware': 620,
        },
        'PROXY_POOL_ENABLED': True,
    })

    models = [model.strip() for model in args.models.split(',')]
    for model in models:
        process.settings.set('FEED_URI', f"/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/amazon_data_{model.replace(' ', '_')}.csv")
        process.crawl(AmazonSpider, models=model)
    process.start()

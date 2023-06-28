import argparse
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from fake_useragent import UserAgent

class EbayItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    shipping = scrapy.Field()
    reviews = scrapy.Field()

class EbaySpider(scrapy.Spider):
    name = "ebay_spider"
    max_pages = 5 # the maximum number of pages to scrape for each model

    def __init__(self, models=None, *args, **kwargs):
        super(EbaySpider, self).__init__(*args, **kwargs)
        self.start_urls = [f"https://www.ebay.com/sch/i.html?_nkw={model}" for model in models.split(',')]
        self.ua = UserAgent()

    def parse(self, response):
        for item in response.css('li.s-item'):
            l = ItemLoader(item = EbayItem(), selector=item)
            l.add_value('model', response.css('input#gh-ac::attr(value)').get())
            l.add_css('title', '.s-item__title::text')
            l.add_css('price', '.s-item__price::text')
            l.add_css('shipping', '.s-item__shipping::text')
            l.add_css('reviews', '.s-item__reviews::text')

            yield l.load_item()

        # handling pagination
        next_page_url = response.css('.x-pagination__control .x-pagination__ol a[rel="next"]::attr(href)').get()
        page_number = response.css('.x-pagination__ol .x-pagination__li--selected::text').get()
        if next_page_url and int(page_number) <= self.max_pages:
            yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse, headers={'User-Agent': self.ua.random})

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, headers={'User-Agent': self.ua.random})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape eBay for product info")
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
        process.settings.set('FEED_URI', f"/Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/data/raw/ebay_data_{model.replace(' ', '_')}.csv")
        process.crawl(EbaySpider, models=model)
    process.start()

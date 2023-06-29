import argparse
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess

class EbayItem(scrapy.Item):
    model = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    reviews = scrapy.Field()

class EbaySpider(scrapy.Spider):
    name = "ebay_spider"
    max_pages = 5  # the maximum number of pages to scrape for each model

    def __init__(self, models=None, *args, **kwargs):
        super(EbaySpider, self).__init__(*args, **kwargs)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape eBay for product info")
    parser.add_argument("models", help="Models to search for (comma-separated)")
    args = parser.parse_args()

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_FIELDS': ['model', 'title', 'price', 'rating', 'reviews']  # specify the order of fields
    })

    models = [model.strip() for model in args.models.split(',')]
    process.crawl(EbaySpider, models=models)
    process.start()
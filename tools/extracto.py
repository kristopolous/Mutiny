#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.8"
# dependencies = [
#  "scrapy",
# ]
# ///
import scrapy
import argparse
import json
import os
import pprint
from scrapy.crawler import CrawlerProcess
from scrapy import signals

class ExtractoSpider(scrapy.Spider):
    name = "extracto"

    def __init__(self, file_path=None, *args, **kwargs):
        super(ExtractoSpider, self).__init__(*args, **kwargs)
        self.file_path = file_path

    def start_requests(self):
        file_path = os.path.abspath(self.file_path)
        with open(file_path, 'r') as f:
            html_content = f.read()
        yield scrapy.Request(url="file://"+self.file_path, callback=self.parse, dont_filter=True)

    def parse(self, response):
        # Implement the scraping logic here
        tralbum_data_json = response.css('script[data-tralbum]::attr(data-tralbum)').get()
        tralbum_data = json.loads(tralbum_data_json)
        pprint.pprint(tralbum_data)
        track_list = [track['title'] for track in tralbum_data['trackinfo']]
        yield {
            'artist': tralbum_data['artist'],
            'date':   tralbum_data['album_release_date'] or tralbum_data['current']['release_date'],
            'title': tralbum_data['current']['title'],
            'about': tralbum_data['current']['about'],
            'tracks': track_list,
        }

def main():
    parser = argparse.ArgumentParser(description="Extract content from a file using Scrapy.")
    parser.add_argument("file_path", help="The path to the file to extract content from.")
    args = parser.parse_args()

    # Scrapy settings
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0',
        'LOG_LEVEL': 'INFO'
    })

    results = []

    def item_scraped(item, response, spider):
        results.append(item)

    process.crawl(ExtractoSpider, file_path=args.file_path)
    crawler = next(iter(process.crawlers))
    for crawler_instance in process.crawlers:
        crawler_instance.signals.connect(item_scraped, signal=signals.item_scraped)
    process.start() # the script will block here until the crawling is finished

    # Convert the extracted data to JSON and print it
    print(json.dumps(results, indent=4))

if __name__ == "__main__":
    main()

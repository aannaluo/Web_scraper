import json
import time
import pandas as pd
import scrapy
from scrapy import Spider, signals
import gspread
from google.oauth2.service_account import Credentials


class AncSpiderSpider(scrapy.Spider):
    name = "anc_spider_master"
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 0.5,
    }
    headers = {
        'accept-language': 'en-US,en;q=0.9',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.final_data = []
        self.json_file = {
            "type": "service_account",
            "project_id": "<redacted>",
            "private_key_id": "<redacted>",
            "private_key": "<redacted>",
            "client_email": "<redacted>",
            "client_id": "<redacted>",
            "auth_uri": "<redacted>",
            "token_uri": "<redacted>",
            "auth_provider_x509_cert_url": "<redacted>",
            "client_x509_cert_url": "<redacted>",
            "universe_domain": "googleapis.com"
        }
        self.sheet_data = self.get_sheet()

    def start_requests(self):
        row = self.sheet_data.pop(0)
        if row['Registration Link']:
            url = 'https://anc.ca.apm.activecommunities.com/vancouver/rest/activity/detail/{}'
            key = row['Registration Link'].split('/')[-1].strip()
            yield scrapy.Request(url.format(key), headers=self.headers, meta={'row': row}, dont_filter=True)
        else:
            self.final_data.append(row)

    def parse(self, response, **kwargs):
        data = json.loads(response.text).get('body', {}).get('detail', {}).get('space_status', '')
        value = 0
        if 'openings' in data or 'opening' in data:
            value = int(data.split()[0].strip())
        row = response.meta['row']
        row['Status'] = f'{value} spots left'
        self.final_data.append(row)

    def parse_again(self, response):
        row = response.meta['row']
        if ('http' in row['Registration Link'] or 'www' in row['Registration Link']) and row['Registration Link'] and \
                row['Registration Link'].lower().strip() != 'program link' and row[
            'Registration Link'].lower().strip() != 'total':
            url = 'https://anc.ca.apm.activecommunities.com/vancouver/rest/activity/detail/{}'
            key = row['Registration Link'].split('/')[-1].strip()
            yield scrapy.Request(url.format(key), headers=self.headers, meta={'row': row})

        else:
            self.final_data.append(row)

    def get_sheet(self):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key("<redacted>")
        worksheet = sheet.worksheet('LIVE DATA')
        data = worksheet.get_all_records()
        return data

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AncSpiderSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self, spider):
        if self.sheet_data:
            row = self.sheet_data.pop(0)
            request = scrapy.Request(url='https://www.example.com', headers=self.headers, meta={'row': row},
                                     callback=self.parse_again, dont_filter=True)
            self.crawler.engine.crawl(request)

    def close(self, spider: Spider, reason: str):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key("<redacted>")
        sheet_list = [list(self.final_data[0].keys())]
        for row_data in self.final_data:
            sheet_list.append(list([row_data.get(key, '') for key in sheet_list[0]]))
        sheet.values_update('LIVE DATA!A1', params={'valueInputOption': 'RAW'}, body={'values': sheet_list})

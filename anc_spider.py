import json
import scrapy
from scrapy import Spider, signals
import gspread
from google.oauth2.service_account import Credentials


class AncSpiderSpider(scrapy.Spider):
    name = "anc_spider"
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 1,
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
            "project_id": "silken-facet-398906",
            "private_key_id": "--",
            "private_key": "--",
            "client_email": "--",
            "client_id": "--",
            "auth_uri": "--",
            "token_uri": "--",
            "auth_provider_x509_cert_url": "--",
            "client_x509_cert_url": "--",
            "universe_domain": "googleapis.com"
        }
        self.capacity = 0
        #self.sheet_names = ['Summer (KCC)', 'Summer (KitsCC)', 'Summer (Sports Services)', 'Summer (MPCC)',
        #                    'Summer (WPGCC)', 'Summer (MOCC)', 'Fall', 'Fall Sports Services']
        self.sheet_names = ['Fall', 'Fall Sports Services']
        #self.sheet_names = ['Fall Sports Services']
        self.sheet_name = self.sheet_names.pop(0)
        self.sheet_data = self.get_sheet()

    def start_requests(self):
        row = self.sheet_data.pop(0)
        if row['Capacity'] and isinstance(row['Capacity'], int):
            self.capacity = int(row['Capacity'])
        if list(row.values())[0]:
            url = 'https://anc.ca.apm.activecommunities.com/vancouver/rest/activity/detail/{}'
            key = list(row.values())[0].split('/')[-1].strip()
            yield scrapy.Request(url.format(key), headers=self.headers, meta={'row': row}, dont_filter=True)
        else:
            self.final_data.append(row)

    def parse(self, response, **kwargs):
        data = json.loads(response.text).get('body', {}).get('detail', {}).get('space_status', '')
        value = 0
        if 'openings' in data or 'opening' in data:
            value = int(data.split()[0].strip())
        if 'Closed' in data:
            value = -1 
        row = response.meta['row']
        if value > -1:
            row['Old'] = int(row['Students'])
            row['Students'] = self.capacity - value
        self.final_data.append(row)

    def parse_again(self, response):
        row = response.meta['row']
        if ('http' in list(row.values())[0] or 'www' in list(row.values())[0]) and list(row.values())[0] and \
                list(row.values())[0].lower().strip() != 'program link' and list(row.values())[
            0].lower().strip() != 'total':
            url = 'https://anc.ca.apm.activecommunities.com/vancouver/rest/activity/detail/{}'
            key = list(row.values())[0].split('/')[-1].strip()
            yield scrapy.Request(url.format(key), headers=self.headers, meta={'row': row}, dont_filter=True)
        else:
            self.final_data.append(row)

    def get_sheet(self):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key("1vcQfIvfz3pdPgJ7FEQB_tmTUgyNhbR0WSD5FoKQWgks")
        worksheet = sheet.worksheet(self.sheet_name)
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
            if row['Capacity'] and isinstance(row['Capacity'], int):
                self.capacity = int(row['Capacity'])
            request = scrapy.Request(url='https://www.example.com', headers=self.headers, meta={'row': row},
                                     callback=self.parse_again, dont_filter=True)
            self.crawler.engine.crawl(request)
        elif self.sheet_names:
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
            gc = gspread.authorize(credentials)
            sheet = gc.open_by_key('1vcQfIvfz3pdPgJ7FEQB_tmTUgyNhbR0WSD5FoKQWgks')  # specify the worksheet name
            column_old = [['Old']]
            column_l_data = [['Students']]
            for row_data in self.final_data:
                column_l_data.append([row_data.get('Students', '')])
                column_old.append([row_data.get('Old', '')])
            sheet.values_update(f'{self.sheet_name}!H1', params={'valueInputOption': 'RAW'},
                                body={'values': column_l_data})
            sheet.values_update(f'{self.sheet_name}!L1', params={'valueInputOption': 'RAW'},
                                body={'values': column_old})
            self.sheet_name = self.sheet_names.pop(0)
            self.final_data.clear()
            self.sheet_data.clear()
            self.sheet_data = self.get_sheet()
            row = self.sheet_data.pop(0)
            if row['Capacity'] and isinstance(row['Capacity'], int):
                self.capacity = int(row['Capacity'])
            request = scrapy.Request(url='https://www.example.com', headers=self.headers, meta={'row': row},
                                     callback=self.parse_again, dont_filter=True)
            self.crawler.engine.crawl(request)

    def close(self, spider: Spider, reason: str):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key('1vcQfIvfz3pdPgJ7FEQB_tmTUgyNhbR0WSD5FoKQWgks')  # specify the worksheet name
        column_old = [['Old']]
        column_l_data = [['Students']]
        for row_data in self.final_data:
            column_l_data.append([row_data.get('Students', '')])
            column_old.append([row_data.get('Old', '')])
        sheet.values_update(f'{self.sheet_name}!H1', params={'valueInputOption': 'RAW'},
                            body={'values': column_l_data})
        sheet.values_update(f'{self.sheet_name}!L1', params={'valueInputOption': 'RAW'},
                            body={'values': column_old})

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
            "project_id": "<redacted>",
            "private_key_id": "<redacted>",
            "private_key": "-----BEGIN PRIVATE KEY-----\n<redacted>\n-----END PRIVATE KEY-----\n",
            "client_email": "<redacted>",
            "client_id": "<redacted>",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/<redacted>",
            "universe_domain": "googleapis.com"
        }
        self.capacity = 0
        #self.sheet_names = ['Summer (KCC)', 'Summer (KitsCC)', 'Summer (Sports Services)', 'Summer (MPCC)',
        #                    'Summer (WPGCC)', 'Summer (MOCC)', 'Fall', 'Fall Sports Services']
        # self.sheet_names = ['Fall', 'Fall Sports Services']
        # self.sheet_names = ['Testing Waitlist']
        self.sheet_names = ['Winter']
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
        waitlist_data = json.loads(response.text).get('body', {}).get('detail', {}).get('space_message', '')
        row = response.meta['row']
        value = 0

        if 'openings' in data or 'opening' in data:
            value = int(data.split()[0].strip())
        if 'Closed' in data:
            value = -1 
        if 'Tentative' in data:
            value = -1
        if value > -1:
            row['Old'] = int(row['Students'])
            row['Students'] = self.capacity - value
            row['Waitlist'] = self.get_waitlist_info(waitlist_data)
            # if waitlist_data is not None:
                # row['Waitlist'] = waitlist_data
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

    def get_waitlist_info(self, page_content):

        # waitlist_split will be a list of the words in the waitlist section
        waitlist_split = page_content.split()


        # if there is a waitlist with at least one person, the length of the sentence will be more than 5 words
        if len(waitlist_split) > 5:

            # for 1 person in the waitlist, activenet writes is in words, for all other numbers it's a number
            waitlist_number = waitlist_split[5].strip()
            if waitlist_number == 'One':
                waitlist_number = 1
            else:
                waitlist_number = int(waitlist_number)
            
            return waitlist_number

        # If waitlist is less than 1, return None
        return None

    def get_sheet(self):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key("<redacted>")
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
            self.update_google_sheet()

    def update_google_sheet(self):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_info(self.json_file, scopes=scopes)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key('<redacted>')
        column_old = [['Old']]
        column_l_data = [['Students']]
        column_waitlist = [['Waitlist']]  # Adding column for Waitlist data

        for row_data in self.final_data:
            column_l_data.append([row_data.get('Students', '')])
            column_old.append([row_data.get('Old', '')])
            column_waitlist.append([row_data.get('Waitlist', '')])  # Add Waitlist info

        sheet.values_update(f'{self.sheet_name}!H1', params={'valueInputOption': 'RAW'},
                            body={'values': column_l_data})
        sheet.values_update(f'{self.sheet_name}!L1', params={'valueInputOption': 'RAW'},
                            body={'values': column_old})
        sheet.values_update(f'{self.sheet_name}!K1', params={'valueInputOption': 'RAW'},
                            body={'values': column_waitlist})  # Update Waitlist column

    def close(self, spider: Spider, reason: str):
        self.update_google_sheet()

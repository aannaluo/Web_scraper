# Web Scraper
Web scraper built for Summer Smash Tennis to gather information from ActiveNet to update registration information.

The python scrapy library was used to build two spiders to update our two separate internal records. 

anc_spider.py is used to keep track of registration numbers for our programs which help us schedule coaches, keep track of revenue, and student attendance.

anc_spider_master.py is used to indicate the number of openings left, and updates our summersmashtennis.ca website with correct numbers.

The authorization key and other sensitive information to our actual spreadsheet has been removed from the script in case of outside parties updating our sheet

import requests
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv
import os
load_dotenv(override=True)

from seleniumbase import SB

crawlbot_server = os.getenv('CRAWLBOT_SERVER')

class Mogi:
    def crawl_url(self,page,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_url?page={page}')
        return url.json()

    def crawl_data_by_url(self,url,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_data_by_url?url={url}')
        return url.json()


class Batdongsan:
    def crawl_url(self,page,proxy=None):
        if proxy is not None:
            url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}&proxy={proxy}')
        else:
            url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}')
        if url.status_code == 200:
            return list(url.json())
        else:
            print('Error when crawl url from batdongsan.com.vn, status code: ', url.status_code)
            return []


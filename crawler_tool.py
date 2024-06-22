import requests
from bs4 import BeautifulSoup
import time
from dotenv import load_dotenv
import os
load_dotenv(override=True)

from seleniumbase import SB
from consume.utils import  Redis, Kafka

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

    def crawl_data_by_url(self,url,proxy=None):
        if proxy is not None:
            data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}&proxy={proxy}')
        else:
            data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}')
        if data.status_code == 200:
            return data.json()
        else:
            print('Error when crawl data by url from batdongsan.com.vn, status code: ', data.status_code)
            return None

class Muaban:
    def crawl_id(self,offset,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_id?offset={offset}')
        return url.json()

    def crawl_data_by_id(self, id, proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_data_by_id?id={id}')
        return url.json()


def crawl_batdongsan_by_url(url):
    data = Batdongsan().crawl_data_by_url(url)
    if data is not None:
        if Kafka().send_data(data,'raw_batdongsan') == True:
            Redis().add_id_to_set(url, 'raw_batdongsan')

    return data

def crawl_muaban_by_id(id):
    if Redis().check_id_exist(id, 'raw_muaban'):
        print("Existed Crawled Id")
        return
    data = Muaban().crawl_data_by_id(id)
    if data is not None:
        if Kafka().send_data(data,'raw_muaban') == True:
            Redis().add_id_to_set(id, 'raw_muaban')
    else:
        print('crawl fail : ', id)

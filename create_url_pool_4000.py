from tqdm import tqdm
from crawler_tool import Batdongsan, crawl_batdongsan_by_url
import pymongo
from pymongo import InsertOne
from dotenv import load_dotenv
from seleniumbase import SB
import time
from bs4 import BeautifulSoup
from datetime import datetime
import os
import math
load_dotenv(override=True)

connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

collection = __database["realestate_url_pool"]

lst_proxy = []
with open(r"./ip.txt", 'r') as f:
    for line in f:
        lst_proxy.append(line.strip())

def crawl_bds_url(page):
    with SB(uc=True,headless=True,block_images=True,proxy_bypass_list=lst_proxy,time_limit=30) as sb:
        if page == 0 or page == 1:
            url = 'https://batdongsan.com.vn/nha-dat-ban?sortValue=1'
        else:
            url = f'https://batdongsan.com.vn/nha-dat-ban/p{page}?sortValue=1'
        sb.open(url)
        time.sleep(1)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')

        links = soup.find_all('a', class_='js__product-link-for-product-id')
        links = ['https://batdongsan.com.vn'+link['href'] for link in links]
    return links

list_url = []
for page in tqdm(range(3000,4000)):
    try:
        list_page = crawl_bds_url(page)
        list_url += list_page
        print("Crawl:", len(list_url))
        if len(list_url) >= 30:
            print(f"Insert: {len(list_url)} urls")
            operations = []
            for url in list_url:
                operations.append(
                    InsertOne({
                        "crawl_at": datetime.now(),
                        "url": url,
                        "source": "batdongsan"
                    })
                )
            if len(operations):
                collection.bulk_write(operations,ordered=False)

            list_url = []
    except Exception as e:
        print(e)

if len(list_url):
    print(f"Insert: {len(list_url)} urls")
    operations = []
    for url in list_url:
        operations.append(
            InsertOne({
                "crawl_at": datetime.now(),
                "url": url,
                "source": "batdongsan"
            })
        )
    if len(operations):
        collection.bulk_write(operations,ordered=False)


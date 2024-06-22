from tqdm import tqdm
from crawler_tool import Batdongsan, crawl_batdongsan_by_url
import pymongo
from pymongo import InsertOne
from dotenv import load_dotenv
from seleniumbase import SB
import time
from bs4 import BeautifulSoup
from datetime import datetime
import requests
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

def crawl_url(page: int, proxy: str = None):
    if page == 0 or page == 1:
        url = 'https://mogi.vn/mua-nha-dat'
    else:
        url = f"https://mogi.vn/mua-nha-dat?cp={page}"

    payload={}
    headers = {
    'authority': 'mogi.vn',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
    'cache-control': 'max-age=0',
    'cookie': 'UID=bbc08ff3-6555-430a-a664-34d08b247e13; _gcl_au=1.1.1821714249.1693211206; _gid=GA1.2.859459221.1693211206; _fbp=fb.1.1693211206549.2104283531; _dc_gtm_UA-52097568-1=1; __gads=ID=91164ce46772351f-2229e7ee50e3000b:T=1693216290:RT=1693216290:S=ALNI_MZ8b1YqoPo8NZfSOnmAicA61d5eqw; __gpi=UID=00000c35190772a5:T=1693216290:RT=1693216290:S=ALNI_MZUyZlXy1Ao_llYUwEyBOago6Oulw; _gat_UA-52097568-1=1; _ga_EPTMT9HK3X=GS1.1.1693214628.2.1.1693216304.37.0.0; _ga=GA1.1.1441087283.1693211206',
    'if-modified-since': 'Mon, 28 Aug 2023 10:02:14 GMT',
    'referer': f'https://mogi.vn/mua-nha-dat?cp={page-1}',
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
    }
    if proxy is not None:
        response = requests.request("GET", url, headers=headers, data=payload,proxies={'https': proxy})
    else:
        response = requests.request("GET", url, headers=headers, data=payload)

    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", class_="link-overlay")
    links = [link.get("href") for link in links]
    return links

list_url = []
for page in tqdm(range(0, 1000)):
    try:
        list_page = crawl_url(page)
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
                        "source": "mogi"
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
                "source": "mogi"
            })
        )
    if len(operations):
        collection.bulk_write(operations,ordered=False)


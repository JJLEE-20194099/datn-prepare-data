from tqdm import tqdm
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

crawlbot_server = os.getenv('CRAWLBOT_SERVER')

class Muaban:
    def crawl_id(self,offset,proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_id?offset={offset}')
        return url.json()

    def crawl_data_by_id(self, id, proxy=None):
        url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_data_by_id?id={id}')
        return url.json()


connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

collection = __database["realestate_url_pool"]


list_ids = []

for offset in tqdm(range(6000, 91900, 20)):
    try:
        list_id = Muaban().crawl_id(offset)
        list_ids += list_id
        print("Crawl:", len(list_ids))

        if len(list_ids) >= 30:
                print(f"Insert: {len(list_ids)} urls")
                operations = []
                for url in list_ids:
                    operations.append(
                        InsertOne({
                            "crawl_at": datetime.now(),
                            "url": url,
                            "source": "muaban"
                        })
                    )
                if len(operations):
                    collection.bulk_write(operations,ordered=False)

                list_ids = []
    except Exception as e:
        print(e)


if len(list_ids):
    print(f"Insert: {len(list_ids)} urls")
    operations = []
    for url in list_ids:
        operations.append(
            InsertOne({
                "crawl_at": datetime.now(),
                "url": url,
                "source": "muaban"
            })
        )
    if len(operations):
        collection.bulk_write(operations,ordered=False)


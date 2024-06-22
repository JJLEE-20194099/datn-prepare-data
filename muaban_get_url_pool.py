from tqdm import tqdm
from crawler_tool import Batdongsan, crawl_batdongsan_by_url
import pymongo
from pymongo import InsertOne
from dotenv import load_dotenv
from seleniumbase import SB
import time
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import os
import math
load_dotenv(override=True)

connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

collection = __database["realestate_url_pool"]

data = list(collection.find({"source": "muaban"}))

df = pd.DataFrame(data)
print(df.shape)


unique_df = df.drop_duplicates(subset = ['url'], keep ='first')

unique_df.reset_index(drop = True).to_csv('./url_pool.csv', index = False)
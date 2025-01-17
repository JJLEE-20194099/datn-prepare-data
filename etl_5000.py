import pandas as pd
from seleniumbase import SB
import time
from consume.utils import Kafka, Redis
from tqdm import tqdm
from crawler_tool import crawl_batdongsan_by_url

lst_proxy = []
with open(r"./ip.txt", 'r') as f:
    for line in f:
        lst_proxy.append(line.strip())

df = pd.read_csv('./url_pool.csv')
urls = df['url'].tolist()

result = []
for i in tqdm(range(5000)):
    url = urls[i]

    try:
        data = crawl_batdongsan_by_url(url)
        time.sleep(3)
    except Exception as e:
        print(e)
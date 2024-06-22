import pandas as pd
from seleniumbase import SB
import time
from consume.utils import Kafka, Redis
from tqdm import tqdm
from crawler_tool import crawl_muaban_by_id

lst_proxy = []
with open(r"./ip.txt", 'r') as f:
    for line in f:
        lst_proxy.append(line.strip())

df = pd.read_csv('./url_pool.csv')
urls = df['url'].tolist()

result = []
for url in urls:
    try:
        data = crawl_muaban_by_id(url)
        time.sleep(5)
    except Exception as e:
        print(e)
import pandas as pd
from seleniumbase import SB
import time
from consume.utils import Kafka, Redis
from tqdm import tqdm
from crawler_tool import crawl_meeyland_by_page


for page in tqdm(range(10000)):
    try:
        data = crawl_meeyland_by_page(page)
        time.sleep(5)
    except Exception as e:
        print(e)
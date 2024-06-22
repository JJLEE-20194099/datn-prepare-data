from fastapi import APIRouter, Depends, BackgroundTasks, FastAPI
from seleniumbase import SB
import time
from bs4 import BeautifulSoup
import requests
import uvicorn
import threading
from tqdm import tqdm

from crawler_tool import Batdongsan, crawl_batdongsan_by_url
from consume.utils import Redis

lst_proxy = []
with open(r"./ip.txt", 'r') as f:
    for line in f:
        lst_proxy.append(line.strip())

proxies = {
        "http": lst_proxy
    }

app = FastAPI()

def get_key_meeyland():
    url = 'https://meeyland.com/mua-ban-nha-dat'
    response = requests.get(url)
    if response.status_code != 200:
        return get_key_meeyland()
    soup = BeautifulSoup(response.text, 'html.parser')
    scripts = soup.find_all('script')
    for script in scripts:
        if '_buildManifest.js' in str(script.get('src')):
                return script.get('src').split('/')[5]

global key_meeyland
key_meeyland = get_key_meeyland()

@app.get("/mogi/crawl_url",tags=["mogi.vn"])
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

@app.get("/mogi/crawl_data_by_url",tags=["mogi.vn"])
def crawl_data_by_url(url: str, proxy: str = None):
    payload={}
    headers = {
    'authority': 'mogi.vn',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
    'cookie': 'UID=bbc08ff3-6555-430a-a664-34d08b247e13; _gcl_au=1.1.1821714249.1693211206; _gid=GA1.2.859459221.1693211206; _fbp=fb.1.1693211206549.2104283531; __gads=ID=91164ce46772351f-2229e7ee50e3000b:T=1693216290:RT=1693216290:S=ALNI_MZ8b1YqoPo8NZfSOnmAicA61d5eqw; __gpi=UID=00000c35190772a5:T=1693216290:RT=1693216290:S=ALNI_MZUyZlXy1Ao_llYUwEyBOago6Oulw; _ga_EPTMT9HK3X=GS1.1.1693214628.2.1.1693216313.28.0.0; _ga=GA1.2.1441087283.1693211206',
    'referer': 'https://mogi.vn/mua-nha-dat',
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
    response = requests.request("GET", url, headers=headers, data=payload, proxies = proxies)
    if response.status_code == 200:
        return {
            'success': True,
            'url': url,
            'html_source': response.text
        }
    else:
        print('Error when crawl data by url from mogi.vn, status code: ', response.status_code)
        return None


@app.get("/batdongsan/crawl_url",tags=["batdongsan.com.vn"])
def crawl_url(page: int,proxy: str = None):

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

@app.get("/batdongsan/crawl_data_by_url",tags=["batdongsan.com.vn"])
def crawl_data_by_url(url: str, proxy: str = None):
    with SB(uc=True,block_images=True,headless=True, proxy_bypass_list=lst_proxy,time_limit=30) as sb:
        sb.open(url)
        time.sleep(1)
        html_content = sb.get_page_source()
    return {
        'success': True,
        'url': url,
        'html_source': html_content
    }


def crawl_bds():
    list_url = []
    for page in tqdm(range(1,200,1)):
        list_page = Batdongsan().crawl_url(page, proxy=True)
        list_url.extend(list_page)
    print('Before : ', len(list_url), ' url')
    list_url_not_crawl = [url for url in list_url if Redis().check_id_exist(url, 'raw_batdongsan') == False]
    print('After : ', len(list_url_not_crawl), ' url')

    for url in tqdm(list_url_not_crawl):
        t = crawl_batdongsan_by_url(url)


@app.get("/batdongsan/crawl-streaming")
def crawl_streaming(background_tasks: BackgroundTasks):

    crawl_bds()
    return "processing"

@app.get("/muaban/crawl_id",tags=["muaban.net"])
def crawl_id(offset: int,proxy: str = None):
    url = f'https://muaban.net/listing/v1/classifieds/listing?subcategory_id=169&category_id=33&sort=1&limit=20&offset={offset}'

    print(url)
    response = requests.get(url,proxies=proxies)

    if response.status_code == 200:
        if 'items' in response.json():
            return [item['id'] for item in response.json()['items']]
        else:
            print('No items in response json when crawl id from muaban.net')
            return []
    else:
        print('Error when crawl id from muaban.net, status code: ', response.status_code)
        return []

@app.get("/muaban/crawl_data_by_id",tags=["muaban.net"])
def crawl_data_by_id(id: str,proxy: str = None):
    url = f'https://muaban.net/listing/v1/classifieds/{id}/detail'
    response = requests.get(url,proxies=proxies)

    if response.status_code == 200:
        return response.json()
    else:
        print('Error when crawl data by id from muaban.net, status code: ', response.status_code)
        return None

@app.get("/meeyland/crawl_id",tags=["meeyland.com"])
def crawl_data(page: int,proxy: str = None):
    if page == 0 or page == 1:
        url = f'https://meeyland.com/_next/data/{key_meeyland}/mua-ban-nha-dat.json?filter=need%5B%5D%3Dcan_ban&page=1&sort=0&category=mua-ban-nha-dat'
    else:
        url = f'https://meeyland.com/_next/data/{key_meeyland}/mua-ban-nha-dat.json?filter=need%5B%5D%3Dcan_ban&page={page}&sort=0&category=mua-ban-nha-dat'

    if proxy is not None:
        response = requests.request("GET", url,proxies={'https': proxy})
    else:
        response = requests.request("GET", url)
    if response.status_code == 200:
        data = response.json()
        data = data['pageProps']['fallback']
        return data[list(data.keys())[0]]['data']
    else:
        print('Error when crawl id from meeyland.com, status code: ', response.status_code)
        return []

@app.get("/meeyland/renew_key",tags=["meeyland.com"])
def renew_key():
    key_meeyland = get_key_meeyland()
    return key_meeyland

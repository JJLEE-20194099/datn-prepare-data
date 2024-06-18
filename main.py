from fastapi import FastAPI
from seleniumbase import SB
import time
from bs4 import BeautifulSoup
import requests
import uvicorn
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

@app.get("/nhatot/crawl_id",tags=["nhatot.com"])
def crawl_id(page: int,proxy: str = None):
    with SB(uc=True,headless=True,block_images=True,time_limit=10) as sb:
        if page == 1 or page == 0:
            url = 'https://www.nhatot.com/mua-ban-bat-dong-san'
        else:
            url = f'https://www.nhatot.com/mua-ban-bat-dong-san?page={page}'

        sb.open(url)
        time.sleep(1)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')

        print(soup)
        ids = soup.find_all('a', class_='AdItem_adItem__gDDQT')
        ids = [link['href'] for link in ids]
        ids = [link.split('/')[-1] for link in ids]
        ids = [id.split('.')[0] for id in ids]
    return {
        'success': True,
        'ids': ids
    }

@app.get("/nhatot/crawl_data_by_id",tags=["nhatot.com"])
def crawl_data_by_id(id: str,proxy: str = None):
    categorys = [1010,1020,1040,1030]
    url = f'https://gateway.chotot.com/v1/public/ad-listing/{id}'
    if proxy is not None:
        response = requests.get(url,proxies={'https': proxy})
    else:
        response = requests.get(url)
    if response.status_code == 200:
        try:
            if response.json()['ad']['category'] in categorys:
                return response.json()
            else:
                return None
        except:
            return None
    else:
        print('Error when crawl data by id from nhatot.com, status code: ', response.status_code)
        return None


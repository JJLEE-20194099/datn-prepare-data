from seleniumbase import SB
import time
from bs4 import BeautifulSoup

lst_proxy = []
with open(r"./ip.txt", 'r') as f:
    for line in f:
        lst_proxy.append(line.strip())

proxies = {
        "http": lst_proxy
    }

page = 2
with SB(uc=True,headless=True,block_images=True, time_limit=30) as sb:
    if page == 0 or page == 1:
        url = 'https://batdongsan.com.vn/nha-dat-ban?sortValue=1'
    else:
        url = f'https://batdongsan.com.vn/nha-dat-ban/p{page}?sortValue=1'
    sb.open(url)
    time.sleep(1)
    html = sb.get_page_source()
    soup = BeautifulSoup(html, 'html.parser')
    print(soup)
    links = soup.find_all('a', class_='js__product-link-for-product-id')
    links = ['https://batdongsan.com.vn'+link['href'] for link in links]
    print(links)



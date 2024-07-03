from clients.discord_client import DiscordClient
import os
import requests
from dotenv import load_dotenv
load_dotenv(override=True)

crawl_webhook_url = os.getenv('HEALTHCHECK_WEBHOOK_URL')

crawl_healthcheck_client = DiscordClient(url = crawl_webhook_url)

def make_requests(url, request_type = "GET", payload = {}, headers = {}):
    return requests.request(request_type, url, headers=headers, data=payload, timeout=10)



def crawl_healthcheck_service():
    url = "http://127.0.0.1:8885/healthcheck"
    response = make_requests(url, request_type = "GET")
    if response.status_code == 200:
        crawl_healthcheck_client.webhook_push_sucess_noti(title = "CRAWLBOT HEALTHCHECK", message = "SERVICE UP")
    else:
        crawl_healthcheck_client.webhook_push_error_noti(title = "CRAWLBOT HEALTHCHECK", message = "SERVICE DOWN")

crawl_healthcheck_service()

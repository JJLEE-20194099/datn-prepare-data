from clients.discord_client import DiscordClient
import os
import requests
from dotenv import load_dotenv
load_dotenv(override=True)

healthcheck_webhook_url = os.getenv('HEALTHCHECK_WEBHOOK_URL')

crawl_healthcheck_client = DiscordClient(url = healthcheck_webhook_url, bot_name="DATN - CRAWLBOT")
feast_healthcheck_client = DiscordClient(url = healthcheck_webhook_url, bot_name="DATN - FEAST")
mlflow_healthcheck_client = DiscordClient(url = healthcheck_webhook_url, bot_name="DATN - MLFLOW")



def make_requests(url, request_type = "GET", payload = {}, headers = {}):
    return requests.request(request_type, url, headers=headers, data=payload, timeout=10)



def crawl_healthcheck_service():
    url = f"{os.getenv('CRAWLBOT_SERVER')}/healthcheck"
    try:
        response = make_requests(url, request_type = "GET")
        if response.status_code == 200:
            crawl_healthcheck_client.webhook_push_sucess_noti(title = "CRAWLBOT HEALTHCHECK", message = "SERVICE UP")
            return {
                "CRAWLBOT SERVER": 1
            }
        else:
            crawl_healthcheck_client.webhook_push_error_noti(title = "CRAWLBOT HEALTHCHECK", message = "SERVICE DOWN")
            return {
                "CRAWLBOT SERVER": 0
            }
    except Exception as e:
        crawl_healthcheck_client.webhook_push_error_noti(title = "CRAWLBOT HEALTHCHECK", message = f"SERVICE DOWN - {e}")
        return {
            "CRAWLBOT SERVER": 0
        }


def feast_healthcheck_service():
    url_obj_list = [
        {
            "name": "FEAST UI",
            "url": os.getenv('FEAST_UI')
        },
        {
            "name": "FEAST SERVER",
            "url": f"{os.getenv('FEAST_SERVER')}/health"
        },
        {
            "name": "FEAST FAST SERVER",
            "url": f"{os.getenv('FEAST_FAST_SERVER')}/healthcheck"
        }
    ]
    result = {}

    for url_obj in url_obj_list:
        url = url_obj["url"]
        name = url_obj["name"]

        try:
            response = make_requests(url, request_type = "GET")
            if response.status_code == 200:
                feast_healthcheck_client.webhook_push_sucess_noti(title = f"{name} HEALTHCHECK", message = "SERVICE UP")
                result[name] = 1
            else:
                feast_healthcheck_client.webhook_push_error_noti(title = f"{name} HEALTHCHECK", message = "SERVICE DOWN")
                result[name] = 0
        except Exception as e:
            feast_healthcheck_client.webhook_push_error_noti(title = f"{name} HEALTHCHECK", message = f"SERVICE DOWN - {e}")
            result[name] = 0

    return result


def mlflow_healthcheck_service():
    url = f"{os.getenv('MLFLOW_SERVER')}"
    try:
        response = make_requests(url, request_type = "GET")
        if response.status_code == 200:
            crawl_healthcheck_client.webhook_push_sucess_noti(title = "MLFLOW HEALTHCHECK", message = "SERVICE UP")
            return {
                "MLFLOW SERVER": 1
            }
        else:
            crawl_healthcheck_client.webhook_push_error_noti(title = "MLFLOW HEALTHCHECK", message = "SERVICE DOWN")
            return {
                "MLFLOW SERVER": 0
            }
    except Exception as e:
        crawl_healthcheck_client.webhook_push_error_noti(title = "MLFLOW HEALTHCHECK", message = f"SERVICE DOWN - {e}")
        return {
                "MLFLOW SERVER": 0
            }

import math
import json

from text import preprocess_text

data = json.load(open('/home/long/airflow/dags/schema/expectations/address.json', 'r'))

def nan_2_none(obj):
    if isinstance(obj, dict):
        return {k:nan_2_none(v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [nan_2_none(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

def update_district_ward_street_util(city):
    return {
        "district": data[city]['district'],
        "ward": data[city]['ward'],
        "street": data[city]['street']
    }

def update_ward_street_util(city, district):
    full_ward = data[city]["full_ward"]
    ward = data[city]["ward"]
    return {
        "ward": [item for item in ward if f'{preprocess_text(district)} - {preprocess_text(item)}' in full_ward]
    }

def update_street_util(city, district, ward):
    full_street = data[city]["full_street"]
    street = data[city]["street"]
    return {
        "street": [item for item in street if f'{preprocess_text(district)} - {preprocess_text(ward)} - {preprocess_text(item)}' in full_street]
    }



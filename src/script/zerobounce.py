import requests
from airflow.models import Variable
from typing import Dict

zero_bounce_api_key = Variable.get("ZERO_BOUNCE_API_KEY", default_var="your_zero_bounce_api_key")

def extract_nested_value(data: Dict, path: str) -> any:
    keys = path.split(".")
    for key in keys:
        if "[" in key and "]" in key:
            base_key, index = key.split("[")
            index = int(index.strip("]"))
            data = data.get(base_key, [])
            if len(data) > index:
                data = data[index]
            else:
                return None
        else:
            data = data.get(key)
        if data is None:
            return None
    return data

def map_fields(data: Dict, field_mapping: Dict) -> Dict:
    mapped_data = {}
    for source_field, target_field in field_mapping.items():
        value = extract_nested_value(data, source_field)
        if value is not None:
            mapped_data[target_field] = value
    return mapped_data

def validate_email(email: str) -> str:  
    try:  
        # response = requests.get(
        #     "https://api.zerobounce.net/v2/validate", 
        #     params=
        #     {
        #     "email": email,
        #     "api_key": zero_bounce_api_key
        #     }
        # )
        # print(response.status.value)  
        # return response.status.value
        return "valid"
    except Exception as e:  
        print(f"Error validating {email}: {e}")  
        return None  # Or some error indicator 
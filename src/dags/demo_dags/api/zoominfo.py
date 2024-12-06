import requests
from airflow.models import Variable

zero_bounce_api_key = Variable.get("ZOOM_INFO_API_KEY", default_var="your_zoom_info_api_key")

def search_zoom_info(query_params):
    # api_key = 'YOUR_ZOOMINFO_API_KEY'  
    # query_params = {  
    #     'name': 'John Doe',  # Example search by name  
    #     'location': 'New York',  # Additional search filters can be added  
    #     # Add more parameters as required  
    # }  
    # Replace with the actual ZoomInfo endpoint for searching contacts or personal data  
    url = 'https://api.zoominfo.com/search/person'  # Example endpoint  
    
    headers = {  
        'Authorization': f'Bearer {zero_bounce_api_key}',  
        'Content-Type': 'application/json'  
    }  
    
    try:  
        response = requests.get(url, headers=headers, data=query_params)  
        if response.status_code == 200:  
            return response.json()["data"] 
        else:  
            print(f"Failed to retrieve data: {response.status_code} - {response.text}")  
            return None  

    except Exception as e:  
        print(f"An error occurred: {e}")  
        return None  

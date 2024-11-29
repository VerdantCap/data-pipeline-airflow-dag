import os
import http.client  
import json  
from typing import Dict
# from dotenv import load_dotenv

# load_dotenv()

# apikey = os.getenv('SERPER_API_KEY')
apikey = "74c14512d7971750512bc8c18cf07c7216be539f"



def serper_website(url: str) -> Dict:  
    conn = http.client.HTTPSConnection("scrape.serper.dev")  
    payload = json.dumps({  
        "url": url  
    })  
    headers = {  
        'X-API-KEY': apikey,  
        'Content-Type': 'application/json'  
    }  
    
    conn.request("POST", "/", payload, headers)  
    res = conn.getresponse()  
    status_code = res.status  
    data = res.read()  

    if status_code == 200:  
        try:  
            json_data = json.loads(data.decode("utf-8"))  
            return json_data["metadata"]
        except json.JSONDecodeError:  
            print("Error: Unable to decode JSON response")  
    else:  
        print(f"Error: Received status code {status_code}")  
        print(data.decode("utf-8")) 
    
    return False
        
def serper_query(query: str) -> Dict:
    
    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps({
        "q": query
    })
    headers = {
        'X-API-KEY': apikey,
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    status_code = res.status  
    data = res.read()  

    if status_code == 200:  
        try:  
            json_data = json.loads(data.decode("utf-8"))  
            print(json_data)  
        except json.JSONDecodeError:  
            print("Error: Unable to decode JSON response")  
    else:  
        print(f"Error: Received status code {status_code}")  
        print(data.decode("utf-8")) 
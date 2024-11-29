import http.client
import json
import os
from dotenv import load_dotenv

load_dotenv()

apikey = os.getenv('SERPER_API_KEY')

import http.client  
import json  

def serper_website(url):  
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
            return json_data
        except json.JSONDecodeError:  
            print("Error: Unable to decode JSON response")  
    else:  
        print(f"Error: Received status code {status_code}")  
        print(data.decode("utf-8")) 
    
    return False
        
def serper_query(query):
    
    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps({
        "q": "apple inc"
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
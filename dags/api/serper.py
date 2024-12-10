import http.client  
import json
from airflow.models import Variable
from typing import Dict


WEBSITE_FILED_MAPPING = {
    "text": "Company Overview",
    "metadata.title" : "Company Title",
    "metadata.description" : "Company Description",
    "metadata.keywords": "Company Keywords",
}

serper_api_key = Variable.get("SERPER_API_KEY", default_var="your_default_secret_key")

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

def serper_website(url: str) -> Dict:  
    # conn = http.client.HTTPSConnection("scrape.serper.dev")  
    # payload = json.dumps({  
    #     "url": url  
    # })  
    # headers = {  
    #     'X-API-KEY': serper_api_key,  
    #     'Content-Type': 'application/json'  
    # }  
    
    # conn.request("POST", "/", payload, headers)  
    # res = conn.getresponse()  
    # status_code = res.status  
    # data = res.read()  

    # if status_code == 200:  
    #     try:  
    #         json_data = json.loads(data.decode("utf-8"))  
    #         return map_fields(json_data, WEBSITE_FILED_MAPPING)
    #     except json.JSONDecodeError:  
    #         print("Error: Unable to decode JSON response")  
    # else:  
    #     print(f"Error: Received status code {status_code}")  
    #     print(data.decode("utf-8")) 
    
    # return False
    data = {
        "text": "Microsoft – AI, Cloud, Productivity, Computing, Gaming & Apps\n\nCyber Week deals\n\nUnwrap savings on select Surface devices, Xbox consoles, accessories, and more. Plus, get fast, free shipping, extended returns, and extended price protection.\n\n\nUp to $520 off\n\n\nSurface Pro, Copilot+ PC\n\nSave now on AI-powered speed, flexibility, and incredible battery life.\n\nSave $339\n\n\nSurface Pro, Copilot+ PC Bundle\n\nSave when you pair the AI-powered speed of Surface Pro 11 with a sleek Pro Keyboard. Plus, get extra savings on select accessories and protection plans.\n\nSave up to $500\n\n\nSurface Laptop, Copilot+ PC\n\nNext-level savings on style, speed, and game-changing AI experiences.\n\nUp to $600 off\n\n\nSurface Laptop Studio 2\n\nThe wait is over—save big on the ultimate powerhouse all-in-one laptop.\n\nCyber Week\n\n\nPurchase a select Xbox Series X, get a bonus console wrap\n\nBuy the Xbox Series X – 1TB Digital Edition (White) and get a bonus Xbox Series X Console Wrap - Call of Duty®: Black Ops 6. Limited-time offer.\n\n$50 off\n\n\nXbox Series S – 512GB (White)\n\nCyber Week is here—save big on the smallest, sleekest Xbox ever.\n\nSave $10\n\n\nXbox Wireless Controllers\n\nLight up the season with pro controllers made to elevate your gameplay.\n\n15% off\n\n\nCall of Duty®: Black Ops 6 - Cross-Gen Bundle\n\nFor a limited time, save on the new spy action thriller—optimized for both Xbox Series X|S and Xbox One.\n\n\nGame Pass\n\n\nUnwrap 100+ high-quality games on console, PC, and cloud for one low monthly price.\n\n\nFor business\n\n\nSave up to $300 on Surface Laptop 6 for Business\n\nBoost your productivity and creativity with top-tier performance, collaboration tools, and advanced AI features with the latest Intel processors.\n\n\nMicrosoft 365 Copilot\n\nSave time and focus on the things that matter most with AI in Microsoft 365 for business.\n\n\nGet Microsoft Teams for your business\n\nOnline meetings, chat, real-time collaboration, and shared cloud storage—all in one place.\n\n\nWindows 11 for business\n\nDesigned for hybrid work. Powerful for employees. Consistent for IT. Secure for all.\n\n\nExplore more about AI and Copilot\n\n\nFighting deepfakes and disinformation\n\nMicrosoft is developing tools and practices to help identify AI-manipulated content—and bring more transparency to digital media.\n\n\nGet creative with Copilot\n\nFrom baking to business marketing, here’s a look at nine clever ways people are tackling their days with AI.\n\n\nRevolutionizing work with Copilot\n\nFrom finance to vet medicine and beyond, workers in various roles are using Copilot to do more in less time.\n\nSlide %{start} of %{total}. %{slideTitle}\n\nSkip human-interest articles and stories slideshow: navigate using the slide tabs\n\n\nBuilding trust in our shared future\n\n\nMicrosoft is committed to using AI to unlock opportunities, drive sustainable progress, and support a world where everyone can flourish\n\nEnd of human-interest articles and stories slideshow: navigate using the slide tabs section",
        "metadata": {
            "title": "Microsoft – AI, Cloud, Productivity, Computing, Gaming &amp; Apps",
            "description": "Explore Microsoft products and services and support for your home or business. Shop Microsoft 365, Copilot, Teams, Xbox, Windows, Azure, Surface and more.",
            "twitter:title": "Microsoft – AI, Cloud, Productivity, Computing, Gaming &amp; Apps",
            "twitter:description": "Explore Microsoft products and services and support for your home or business. Shop Microsoft 365, Copilot, Teams, Xbox, Windows, Azure, Surface and more.",
            "og:url": "https://www.microsoft.com/en-us",
            "og:title": "Microsoft – AI, Cloud, Productivity, Computing, Gaming &amp; Apps",
            "og:description": "Explore Microsoft products and services and support for your home or business. Shop Microsoft 365, Copilot, Teams, Xbox, Windows, Azure, Surface and more.",
            "og:type": "website"
        },
        "credits": 2
    }
    return map_fields(data, WEBSITE_FILED_MAPPING)

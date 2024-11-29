from typing import Dict
import requests

PROFILE_FIELD_MAPPING = {
    "photoUrl": "imgUrl",
    "headline": "headline",
    "location": "location",
    "linkedInUrl": "linkedinProfile",
    "firstName": "First Name",
    "lastName": "Last Name",
    "summary": "description",
    "followerCount": "subscribers",
    "positions.positionHistory[0].title": "jobTitle",
    "positions.positionHistory[1].title": "jobTitle2",
    "positions.positionHistory[0].companyName": "company",
    "positions.positionHistory[1].companyName": "company2",
    "positions.positionHistory[0].linkedInUrl": "companyUrl",
    "positions.positionHistory[1].linkedInUrl": "companyUrl2",
    "positions.positionHistory[0].description": "jobDescription",
    "positions.positionHistory[1].description": "jobDescription2",
    "schools.educationHistory[0].schoolName": "school",
    "schools.educationHistory[1].schoolName": "school2",
    "schools.educationHistory[0].linkedInUrl": "schoolUrl",
    "schools.educationHistory[1].linkedInUrl": "schoolUrl2",
    "skills[0]": "skill1",
    "skills[1]": "skill2",
    "skills[2]": "skill3",
    "skills[3]": "skill4",
    "skills[4]": "skill5",
    "skills[5]": "skill6",
}


ACTIVITIES_FIELD_MAPPING = {
    "posts[0].activityUrl": "postUrl",
    "posts[0].authorPublicIdentifier": "profileUrl",
    "posts[0].text": "postContent",
    "posts[0].activityDate": "postDate",
    "reactions[0].type": "typeField",
    "reactions[0].relatedPost.activityUrl": "sharedPostUrl",
    "reactions[0].relatedPost.authorPublicIdentifier": "sharedPostProfileUrl",
    "reactions[0].relatedPost.text": "postContent",
    "reactions[0].relatedPost.activityDate": "postDate",
}

COMPANY_FIELD_MAPPING = {

}

serper_api_key = "sk_b1edcda9a6bb0ac0d26c9c3936ad36183cf775f2"

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


def search_linkedin_profile(linkedInUrl: str = "https://www.linkedin.com/in/williamhgates") -> dict:
    try:
        url = "https://api.scrapin.io/enrichment/profile"
        querystring = {"apikey":serper_api_key,"linkedInUrl":linkedInUrl}
        response = requests.request("GET", url, params=querystring)
        return map_fields(response.json()["person"], PROFILE_FIELD_MAPPING)
    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None

def search_linkedin_activity(linkedInUrl: str = "https://www.linkedin.com/in/williamhgates") -> dict:
    try:
        url = "https://api.scrapin.io/enrichment/activities"
        querystring = {"apikey":serper_api_key,"linkedInUrl":linkedInUrl}
        response = requests.request("GET", url, params=querystring)
        return map_fields(response.json(),ACTIVITIES_FIELD_MAPPING)
    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None

def search_linkedin_company(linkedInUrl: str = "https://www.linkedin.com/company/1035") -> dict:
    try:
        url = "https://api.scrapin.io/enrichment/company"
        querystring = {"apikey":serper_api_key,"linkedInUrl":linkedInUrl}
        response = requests.request("GET", url, params=querystring)
        return response.json()["company"]
    except Exception as e:
        print(f"Err scraping {linkedInUrl} : {e}")
        return None
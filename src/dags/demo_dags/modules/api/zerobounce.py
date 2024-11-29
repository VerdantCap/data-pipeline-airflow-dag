import requests
zero_bounce_api_key = "d049596d57d549d0ade2bcbf6d158204"


def validate_email(email: str) -> str:  
    try:  
        response = requests.get(
            "https://api.zerobounce.net/v2/validate", 
            params=
            {
            "email": email,
            "api_key": zero_bounce_api_key
            }
        )
        print(response.status.value)  
        return response.status.value  # You can adjust to get other data from the response.  
    except Exception as e:  
        print(f"Error validating {email}: {e}")  
        return None  # Or some error indicator 
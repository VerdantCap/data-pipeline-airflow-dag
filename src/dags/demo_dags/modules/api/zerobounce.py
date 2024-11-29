from zerobouncesdk import ZeroBounce, ZBException

zero_bounce_api_key = "d049596d57d549d0ade2bcbf6d158204"


def validate_email(email: str) -> str:  
    try:  
        zero_bounce = ZeroBounce("d049596d57d549d0ade2bcbf6d158204")  
        response = zero_bounce.validate(email)
        print(response.status.value)  
        return response.status.value  # You can adjust to get other data from the response.  
    except ZBException as e:  
        print(f"Error validating {email}: {e}")  
        return None  # Or some error indicator 
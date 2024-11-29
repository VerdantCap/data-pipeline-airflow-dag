from datetime import datetime, timedelta  
from airflow.decorators import dag, task  
import boto3  
import pandas as pd  
import os  
from airflow.models import Variable
from modules.api.scrapin import search_linkedin_profile, search_linkedin_company, search_linkedin_activity
from modules.api.zerobounce import validate_email
from modules.api.serper import serper_website

DAG_ID = "Validate_CSV_Files"  
S3_BUCKET_NAME = "airbyte-state-dev-us-east-2-genie-platforms"  
S3_DIRECTORY = "air-byte-sync-destination/zoominfo-preview/"  
OUTPUT_DIRECTORY = "air-byte-sync-destination/zoomiinfo-validate/"  

# aws_access_key = Variable.get("AWS_ACCESS_KEY", default_var="your_default_access_key")  
# aws_secret_key = Variable.get("AWS_SECRET_KEY", default_var="your_default_secret_key")  
# aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="you_default_secret_key")

aws_access_key="ASIA4RCAOLGLCEZRJYW5"
aws_secret_key="wtxa0OZi1KAB8tkSO78jDnyOSbTerrRXnIIdOsoQ"
aws_session_token="IQoJb3JpZ2luX2VjENH//////////wEaCXVzLWVhc3QtMiJHMEUCIDUa7HY7Bg8Z8OsR6CkPhyGIXCC6lZFxY3Os8+3SrCxOAiEAhNrElq3AVLnJMuK7dxve/nCeRAVWBVUjkdf4vv2CcP8qmgMIehAAGgw4NjEyNzYxMDEwMTQiDL51jeFsq0iSQPjQnir3An+5BK5ql5JFZndPhK+mpDujjC73+LMH900EWcFNg7CFlY4y70XTjOWVu9+CyhnV3cIsereXQlh8TPR2loqBr+p+kDJUD9jrmyWTasyr00/lGfr3xh6pDjS/vELT2gWhSZ/fCxDXdrKA9mbc61x5BMKEXNxuFtmlcVLKn+Ur4kcnltHgZsmsIyBKju7N6tnBBK8/3HZbLrUHTOdGT7ZHmBQroJ158EoRvm7q4VRXW8XL/+Fg1NovSildMlmO6nZ0BtXMnDCrSfbSx3Knm6svEV3L+t+zBiTtCc8UVY6ENUPM4aMlaUWWCsy9DJ7cK2VkQVZkggfakcq3WqQkwVU6EU8J6UfCtjnFq+AF01QgnnhdRdzpR24i+2JfhOiVakL68bzSAoWKmcZWeM2a2D4JY+BipJKGihOWCr2JTa4OGJdFIvvfAgxhvkkH+n4M4bkPULzm/qtH2/1RE6sIxsDPIqaGs2hBhMkMaNRGbeib/dk6+fubM/Py2zDO3Ke6BjqmAUC4EfvUGl8H7IuYqUxfTdM29Mi3GMPiG4vgQk60aqxiTMtFdO9Qp3Hqu06VlUHV3d7P0/fDrt0oLY36BvvyOq/F/+7Kh2h+i2nhXETF7cj7jNy0+ZA0uVuzOhVimmH1QcIkRqmY4R3Hd9b0BWy7N8FHD82wFv4F5EJKhnQFQ+q+SksjRRB44g2U8b0AjNb8MItCEuFAVFP4BnQqz6wngSPkjcRBDx8="


@dag(  
    dag_id=DAG_ID,  
    schedule_interval="* * * * *",  
    start_date=datetime(2024, 11, 25),  
    dagrun_timeout=timedelta(minutes=5),  
    catchup=False,  
    is_paused_upon_creation=False,  
)  
def process_csv_files():  

    @task  
    def fetch_files_from_s3() -> list:  
        """Retrieve CSV file keys from the designated S3 directory."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
        file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]  
        return file_keys  

    @task  
    def process_files(file_keys: list):  
        """Download, process, and upload each CSV file."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        
        for file_key in file_keys:  
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)  
            data = pd.read_csv(response['Body'])  
            df = data.dropna(subset=["Email Address"])
            df['validation_status'] = df['Email Address'].apply(validate_email)
            df = data.dropna(subset=["LinkedIn Contact Profile URL"])
            df_profile = df['LinkedIn Contact Profile URL'].apply(search_linkedin_profile)
            df_activity = df['LinkedIn Contact Profile URL'].apply(search_linkedin_activity)
            df_serper = df['Website'].apply(serper_website)
            # df['company_data'] = df['LinkedIn Company Profile URL'].apply(search_linkedin_company)
            df = pd.concat([df, df_profile, df_activity, df_serper], join='inner', axis=1)
            csv_buffer = df.to_csv(index=False)
            # try:
            #     data = {
            #         "return_url":"",
            #         "first_name_column" : 2,
            #         "second_name_column" : 3,
            #         "has_header_row" : False,
            #         "remove_duplicate" : True,
            #         "api_key": zero_bounce_api_key,
            #         "email_address_column": 14
            #     }
            #     response = requests.post(
            #         "https://bulkapi.zerobounce.net/v2/sendfile",
            #         data = data,
            #         files = {"file": (os.path.basename(file_key),response['Body'].read(),"text/csv")}
            #     )

            #     if response.json()["success"] == True:
            #         try:
            #             output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"
            #             result = response.get(
            #                 "https://bulkapi.zerobounce.net/v2/getfile",
            #                 params={
            #                     "api_key": zero_bounce_api_key,
            #                     "file_id": response.json()["file_id"],
            #                 },
            #             )
            #             if result.headers["Content-Type"] == "application/json":
            #                 print(result.json())
            #             else:
            #                 s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=result.content)
            #         except Exception as e:
            #              print("ZeroBounce get_file error: " + str(e))
            #     else:
            #         print(file_key,"sending failed.")
            # except Exception as e:
            #     print("ZeroBounce send_file error: " + str(e))
            
            output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)

    @task  
    def start_message():  
        print("Initiating the CSV processing workflow.")  

    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    start = start_message()  
    file_keys = fetch_files_from_s3()  
    process_files(file_keys)  
    end = end_message()  

    start >> file_keys >> process_files(file_keys) >> end  

dag_instance = process_csv_files()  
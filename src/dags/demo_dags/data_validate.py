from datetime import datetime, timedelta  
from airflow.decorators import dag, task  
import boto3  
import pandas as pd  
import os
from airflow.models import Variable
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ''))) 
from api.scrapin import search_linkedin_profile, search_linkedin_company, search_linkedin_activity
from api.zerobounce import validate_email
from api.serper import serper_website

DAG_ID = "Validate_CSV_Files"  
S3_BUCKET_NAME = "airbyte-state-dev-us-east-2-genie-platforms"  
S3_DIRECTORY = "air-byte-sync-destination/zoominfo-preview/"  
OUTPUT_DIRECTORY = "air-byte-sync-destination/zoomiinfo-validate/"  

# aws_access_key = Variable.get("AWS_ACCESS_KEY", default_var="your_default_access_key")  
# aws_secret_key = Variable.get("AWS_SECRET_KEY", default_var="your_default_secret_key")  
# aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="you_default_secret_key")

aws_access_key="ASIA4RCAOLGLE73FIELL"
aws_secret_key="7Zru2lGHCBVOcY/Ka+9pXMh1RJ/L5+EBtuo+ILkZ"
aws_session_token="IQoJb3JpZ2luX2VjEBsaCXVzLWVhc3QtMiJIMEYCIQC9lHN3BOfG7qmy/3+0Ee4E1aTh5wm6oopyHxlxvUfLcQIhAJswsITN5TMMBim/tG30bFBmg/kWjLyug/zxcxplb9y+KqMDCMT//////////wEQABoMODYxMjc2MTAxMDE0Igybp3tI6FHKuoGF+t0q9wJKIbi793zVG4qClZUHvnFaq6tm/ai5vBWMawvWKnpmPUc+n/JT8RvepdqsGDEZL4gaGj0WJBmSlZjgo2o3fMUMCE1a1J2K4AdnuhdcADQQNNTz+oICd9FlX5VXDO0jB0vcbNJucoAxw1gXWTWxUXeNaKMmPItK44IHnCq+qe/P6OdloCdA0a5oEWNHKcA7rOR7nzX95cc1zeOHrJ2CwHZ0FyvJMU3SxbelIrve6o7lqsUSpbTlzVC3oiEaGQDNafrAbvW9DCcwmKo68UgH3MqgbEvZJVj9WyhXlkA/YU9PGhjuoZP68VlSCgLLY7MXDZodqdZjUpDnernwx73AzUKCEYsNx8MrQE4keXovPnyuPCmN8XWH0ojPwH5BNOdhMi9ogIksn0Eeiq6Ot/06ytGwyulpKFF/vwSsdQGJV+4LZoGVdXuzPf+U67/WpMSQZvOZx6/v7bOyGtyCIt9vSGKs9CdMczqRN0pYGlxbkv8pRm4d4uyAOw4wyZC4ugY6pQE+Efjn2QBqaS/oStpa87Kbld2dot1iDavt26Xrh3r1ms2yi1W4dFajYRigXXDCv1ORgJwpDxexJKtoYRg9DaHN23/H/An5yLCdt5fjTW7Ws+/OngJzfDFv8a4ojY3aODtxhhh1LkCEvuGUTizIyPfefHy4ZZ6BWXxmM4I/x+dgY5bFSnn9p0LDIhPzy9Oep+wvJwC2L87rQoTlxaJsvlWxHsCTSwM="

@dag(  
    dag_id=DAG_ID,  
    schedule_interval="*/10 * * * *",  
    start_date=datetime(2024, 11, 25),  
    dagrun_timeout=timedelta(minutes=5),  
    catchup=False,  
    is_paused_upon_creation=False,  
)  
def process_csv_files():  

    # @task  
    # def fetch_files_from_s3() -> list:  
    #     """Retrieve CSV file keys from the designated S3 directory."""  
    #     s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
    #     response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
    #     file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]  
    #     return file_keys  

    @task  
    def start_message():  
        print("Initiating the CSV processing workflow.")  

    @task
    def fetch_file_from_s3(file_key: str) -> pd.DataFrame:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        df = pd.read_csv(response['Body'])
        df = df.dropna(subset=["Email Address", "LinkedIn Contact Profile URL", "Website", "LinkedIn Company Profile URL"])
        return df

    @task
    def process_email(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(list(df['Email Address'].apply(validate_email)))

    @task
    def process_profiles(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_profile)))
    
    @task
    def process_activities(df: pd.DataFrame) -> pd.DataFrame:  
        return pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_activity)))
    
    @task
    def process_companies(df: pd.DataFrame) -> pd.DataFrame:  
        return pd.DataFrame(list(df['LinkedIn Company Profile URL'].apply(search_linkedin_company)))

    @task  
    def process_websites(df: pd.DataFrame) -> pd.DataFrame:  
        return pd.DataFrame(list(df['Website'].apply(serper_website)))

    @task  
    def synthesize_results(file_key: str, df: pd.DataFrame, emails: pd.DataFrame, profiles: pd.DataFrame, activities: pd.DataFrame, websites: pd.DataFrame, companies: pd.DataFrame) -> None:  
        df = pd.concat([df, emails, profiles, activities, websites, companies], join = 'inner', axis=1) 

        csv_buffer = df.to_csv(index=False)  
        output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)  
        print(f"Processed file uploaded to {output_key}")

    # @task  
    # def process_files(file_keys: list):  
    #     """Download, process, and upload each CSV file."""  
    #     s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        
    #     for file_key in file_keys:  
    #         response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)  
    #         df = pd.read_csv(response['Body'])  
    #         # df = data.dropna(subset=["Email Address"])
    #         email= df['Email Address'].apply(validate_email)
    #         # df = data.dropna(subset=["LinkedIn Contact Profile URL"])
    #         df_profile = pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_profile)))
    #         df_activity = pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_activity)))
    #         df_company= pd.DataFrame(list(df['LinkedIn Company Profile URL'].apply(search_linkedin_company)))
    #         df_serper = pd.DataFrame(list(df['Website'].apply(serper_website)))
    #         df_result = pd.concat([df, email, df_profile, df_activity,df_company,df_serper], join='inner', axis=1)
    #         csv_buffer = df_result.to_csv(index=False)            
    #         output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"
    #         s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)

    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
    file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    for i, file_key in enumerate(file_keys):
        start = start_message.override(task_id=f"start_task_{i}")()  
        df = fetch_file_from_s3.override(task_id=f"fecth_file_{i}")(file_key)
        emails = process_profiles.override(task_id=f"valied_email_{i}")(df)
        profiles = process_profiles.override(task_id=f"process_profile_{i}")(df)  
        activities = process_activities.override(task_id=f"process_activities_{i}")(df)  
        companies = process_companies.override(task_id=f"process_companies_{i}")(df)
        websites = process_websites.override(task_id=f"process_websites_{i}")(df)  
        synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_key, df, emails, profiles, activities, companies, websites)
        end = end_message.override(task_id=f"end_task_{file_key}")()
        start >> df >> [emails, profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
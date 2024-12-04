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

aws_access_key="ASIA4RCAOLGLFSB7RZMP"
aws_secret_key="E2FV6nDhgXjBtDN0KCpBbsh0qgOiqF9fPkB7eWAo"
aws_session_token="IQoJb3JpZ2luX2VjEEEaCXVzLWVhc3QtMiJIMEYCIQCW9GabHhcJ7/HvLI+hWJT4FlY78ynYdzaCbxvX//so3QIhAP8dvZXJVB+eosoNpOZ0cQyFVzG8YVWjjdoyXAaRSTz1KqMDCOr//////////wEQABoMODYxMjc2MTAxMDE0Igyyk7SlpHUPDfa8BVcq9wIkEyKs3Kb1huiSuzav37qPTRKskOlMxTR+Mj/Aly/zo2IBGpUpt7zGyBbqqSfEEjKOu+RUCYymOc3dxi2vnZOf5eU8CuMPc9ff2icYiZlhCeOGNZEf/wH8DUOhscisui0VRkp57ZDGM8i8OjttTeRfw1V5TFMP7AyUG3Tm7GQ8T0Zf2Xc1RmKpnzsuc43nPwTVCW5PNeHpIe6FZXPNKhYgKKJO3PDhZuct2hhydZmNk9A3m23QBno5Fc+G3Fi76zT7zWy17LmXUog2jpDP8oBw9hTDCy1CYQtsql9R3p4dlxyJ277EvoDyNXLZZMdLKoSadg5+kcg2yN7dKlxL5U2VnA1qd0lDgC/5TBaLuGjyezRqHVj+BXCDKAMOtALqhLNi1+frnzvmYn5F/pLTC+UdxNRMAmXD8Z06E057M3eLGaZzEPVwyJ4qevikBBxWbOH8D+bexAZxl+Pdz/93GY/63FEa50CdZH7CDvd/r6S+U9tQOZqBGeUwtKjAugY6pQHs6YD0GaINd9IuhjTb2HSMMnKEpYhcyekp9OH3nIjCkzaUt2IbuAF6d1CspVD6FJa1aAyhvaVqSZuxne/TDO92eZWUlFomYLmQgd5hKkcfBGANdOmS+2LfsGV0FQvTX4JFrFxNKAXvfVk92Np8MFaQlWXYc+BBySE3zx0VovMWJBwYLakDGcPG6dbgZzE2ccl5TPHwkQjq+cXN2mAk5rFv02l2WTg="

@dag(  
    dag_id=DAG_ID,  
    schedule_interval="*/10 * * * *",  
    start_date=datetime(2024, 11, 25),  
    dagrun_timeout=timedelta(minutes=5),  
    catchup=False,  
    is_paused_upon_creation=False,  
)  
def process_csv_files():  

    @task  
    def start_message():  
        print("Initiating the CSV processing workflow.")  

    @task
    def fetch_file_from_s3(file_key: str) -> pd.DataFrame:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        df = pd.read_csv(response['Body'])
        df = df.dropna(subset=["Email Address"])
        return df

    @task
    def process_email(df: pd.DataFrame) -> pd.DataFrame:
        emails = pd.DataFrame(list(df['Email Address'].apply(validate_email)))
        df = pd.concat([df, emails], join = 'inner', axis=1) 
        df = df.loc[:, ~df.columns.duplicated()]
        return df[df["validation_status"] == "valid"]

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
    def synthesize_results(file_key: str, df_emails: pd.DataFrame, profiles: pd.DataFrame, activities: pd.DataFrame, websites: pd.DataFrame, companies: pd.DataFrame) -> None:  
        df = pd.concat([df_emails, profiles, activities, websites, companies], join = 'inner', axis=1) 
        df = df.loc[:, ~df.columns.duplicated()]
        csv_buffer = df.to_csv(index=False)  
        output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)  
        print(f"Processed file uploaded to {output_key}")


    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
    file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    for i, file_key in enumerate(file_keys):
        start = start_message.override(task_id=f"start_task_{i}")()  
        df = fetch_file_from_s3.override(task_id=f"fecth_file_{i}")(file_key)
        df_emails = process_email.override(task_id=f"valied_email_{i}")(df)
        profiles = process_profiles.override(task_id=f"process_profile_{i}")(df_emails)  
        activities = process_activities.override(task_id=f"process_activities_{i}")(df_emails)  
        companies = process_companies.override(task_id=f"process_companies_{i}")(df_emails)
        websites = process_websites.override(task_id=f"process_websites_{i}")(df_emails)  
        synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_key, df_emails, profiles, activities, companies, websites)
        end = end_message.override(task_id=f"end_task_{i}")()
        start >> df >>  df_emails >> [ profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
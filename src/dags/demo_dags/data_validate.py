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

aws_access_key="ASIA4RCAOLGLCRIWA4IE"
aws_secret_key="kbXJyMeh80k+bmwFvlO7g21sA4Cme4ZwiSA4NHRo"
aws_session_token="IQoJb3JpZ2luX2VjECgaCXVzLWVhc3QtMiJHMEUCIQDnLHvFolpnqIj7Al45P8nFnp19vQc1aA9Qg+fmxeJ8OAIgeTBH2KgzZpG7qa9HmKygEGFxCTL+G+4YjLbsGFDat5QqowMI0f//////////ARAAGgw4NjEyNzYxMDEwMTQiDIthLpnWqcRk+lXlwyr3Ak13zDe6HK+qhmTArQJy0EYw3uNafFz96a3vAuSVNQeorKnEZ97rotqCy4gt6avY1Ad65tPBnOTMnuTvbiyUc80bC/2AsBjWHJss9fdF3xtjre/0PMSY8gOSqYboNYQviwLMn8708lYs3tyY0uY+OZ/jiTdzl+2VeNEY6Y3eny5Tb/bEI3d30uxPDvU0BaQ6yhS/qFKMbIDphrYuEcJAfHS4ru1NoU5JFnlxF1EpgsAk+09d767lHJdGWM7BlUJ+t0ycplviIeZpvAPiCeDCKbqlMMPOx1ju1wgtdQymGX/QtYUFfCOsdxhrtglDr1+MziUGGhKv4dC/2x+hOPuFX4yu6kCEnUqev5urKgwqxb7TATi8Lm52jBKLEy4ZY89x/HSUOTXFn9vfGfLC8Z7hB4XU/FAL4ciAO5ELIspvlMkS5R7rkVI3Q92EyztPumiCzjczZzw8ZwrzWUiFAbWteZFgnE3PwYiZ08EzaKMwzPPgN6Slwxh8CzCG/bq6BjqmAf6DizGRZmIvDNCaDKUX5jARt5Sl2ZpLR73gGnE7fvWSQhLuKWisJHyXKsGjK6/1Ch+Q9daDbR2ZWMyAJ1MJLwjoCPmqYDochZA6AAWa9tMzX6+8PTxReBY9EIg9+AUQ/gBkddkeNQMBlhsP+r2hPnOwTrg4srf7ThMsc4zbgde0auwYje/B7JgECGFsNEOBv8Sve9cAVfuss2kFq+xXAJXKLSFl1yQ="


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


    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
    file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    for i, file_key in enumerate(file_keys):
        start = start_message.override(task_id=f"start_task_{i}")()  
        df = fetch_file_from_s3.override(task_id=f"fecth_file_{i}")(file_key)
        emails = process_email.override(task_id=f"valied_email_{i}")(df)
        profiles = process_profiles.override(task_id=f"process_profile_{i}")(df)  
        activities = process_activities.override(task_id=f"process_activities_{i}")(df)  
        companies = process_companies.override(task_id=f"process_companies_{i}")(df)
        websites = process_websites.override(task_id=f"process_websites_{i}")(df)  
        synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_key, df, emails, profiles, activities, companies, websites)
        end = end_message.override(task_id=f"end_task_{i}")()
        start >> df >> [emails, profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
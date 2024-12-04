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

aws_access_key="ASIA4RCAOLGLE7S4DS5Y"
aws_secret_key="mt1cY0gCC8l0TeNi1EleMvFbx6uamz5G4WHKXn15"
aws_session_token="IQoJb3JpZ2luX2VjEEkaCXVzLWVhc3QtMiJGMEQCIASoGsNwPxDr0PkZmk6C/M8VSp4pmX7xiHD5z5lEKQfIAiBdWyUj0lt/4mFA4adrsECQH7S+mmHhmeho4tb9+Jet+yqjAwjy//////////8BEAAaDDg2MTI3NjEwMTAxNCIM4hVo7UlmuFg27B/eKvcCp7eWzXA92b8BiEeD4a5H+vOTFQUwau7+Ajzy771WmlYwd+QjCxcmIYV0fYpyKDOVvXxWG05gt9wWVWpMsoHNVJlDhLgouXjK9s+0+Ap3ss5fWc3Z4vizFUMTq/yvTSV0zmPkFksnJdhYqgH4AKjTGK6PhEV2dh4l/2morFZvloe/csuPUAmIo79SvAHK9uidnBHQ9OeUuvTy2kg8b3uGoBi5kdTmXaUCTDRLlgfx9y5sxBa0Z8z7JszteTdaue9X+wSJivnYr/V0uzdmr+LHJzsM3gUGYlbsNzGodCQrhkxn54gFx7vw9MsLFSFN19Q1nB+ywrk1Zg0R7D5LFmIz/J+juPPWxFBbN7Y4bswz8Ve1c7NjSaP19tc+E8QNNSDoMFZbtT+0F1DAPPIaL2YnCqg8BZwvrpvBV6ZORS2pWR8dZ5eo4L7psgIjxO9T+ulsHNLqKsb2KEqIRHinVz7+AjOoug52qqmv3kBZcE192XPu0/Ahsn8bMK6NwroGOqcB2AHNxxI7NwaPygDVXM9Ct7t+R5O156xXHP+D4oh1mHQ645a0MS8yybTGj1FeNOJNzkGMnO5dNX3pDGmIH70FNFU2eS5+mJpM/FNEB0X2p9e0MLu8Hx4ZEwms8sWdOVv6O7+QAZQleXZIz5Ni3bXK+ZtIL09sBEbZsFi8PSkDIRMceeqGIBvUND/+00OaAfx1KnMcqTgCpCtUBTBx64W09Ch9f5pak/E="

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
    def consumer_kafka():
        print("Getting the msgs from kafka")

    @task  
    def start_message(pool="default_pool"):  
        print("Initiating the CSV processing workflow.")  

    @task
    def process_query():
        print("getting basic info from the query")

    @task
    def fetch_file_from_s3(file_key: str, pool="default_pool") -> pd.DataFrame:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        df = pd.read_csv(response['Body'])
        df = df.dropna(subset=["Email Address"])
        return df

    @task
    def process_email(df: pd.DataFrame, pool="default_pool") -> pd.DataFrame:
        emails = pd.DataFrame(list(df['Email Address'].apply(validate_email)))
        df = pd.concat([df, emails], join = 'inner', axis=1) 
        df = df.loc[:, ~df.columns.duplicated()]
        return df[df["validation_status"] == "valid"]

    @task
    def process_profiles(df: pd.DataFrame, pool="default_pool") -> pd.DataFrame:
        return pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_profile)))
    
    @task
    def process_activities(df: pd.DataFrame, pool="default_pool") -> pd.DataFrame:  
        return pd.DataFrame(list(df['LinkedIn Contact Profile URL'].apply(search_linkedin_activity)))
    
    @task
    def process_companies(df: pd.DataFrame, pool="default_pool") -> pd.DataFrame:  
        return pd.DataFrame(list(df['LinkedIn Company Profile URL'].apply(search_linkedin_company)))

    @task  
    def process_websites(df: pd.DataFrame, pool="default_pool") -> pd.DataFrame:  
        return pd.DataFrame(list(df['Website'].apply(serper_website)))

    @task  
    def synthesize_results(file_key: str, df_emails: pd.DataFrame, profiles: pd.DataFrame, activities: pd.DataFrame, websites: pd.DataFrame, companies: pd.DataFrame, pool="default_pool") -> None:  
        df = pd.concat([df_emails, profiles, activities, websites, companies], join = 'inner', axis=1) 
        df = df.loc[:, ~df.columns.duplicated()]
        csv_buffer = df.to_csv(index=False)  
        output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)  
        print(f"Processed file uploaded to {output_key}")


    @task  
    def end_message(pool="default_pool"):  
        print("CSV processing workflow completed.")  

    # @task
    # def fetch_file_keys():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
    file_keys =  [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

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
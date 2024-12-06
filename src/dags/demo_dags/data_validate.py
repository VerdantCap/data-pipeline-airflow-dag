from datetime import datetime, timedelta  
from airflow.decorators import dag, task  
import boto3  
import pandas as pd  
import os
from airflow.models import Variable

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '')))
from api.zoominfo import search_zoom_info 
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

aws_access_key="ASIA4RCAOLGLBMHLBCCW"
aws_secret_key="RgoD1728T9sARYqK/A1NSLq4/8aB1lIzqaOz0hR5"
aws_session_token="IQoJb3JpZ2luX2VjEHcaCXVzLWVhc3QtMiJHMEUCIQCoefYNbzIHskttrGQ0cIWKiSjLqTNzmWQSBdOlnqoXUwIgA0gV0gyKLdRQ2Pb9hFwv0/1sh3a3xWJGscOnGymgWfAqmgMIMBAAGgw4NjEyNzYxMDEwMTQiDH5fpCdq+zHnBEvSdyr3AseDiAEHZnRG/XTxdvZMm6d1K4ltHeaEdq+cqpD+YeT71/J9QChGyid+BJ91vn5B2ROgAHuPS11jjT/MHQGbOAPrUuSIo952pFYCX1+zMuS6N7ywaiCCkjxIvg1PmcuX2Pe/xy3ZDA3c0zvJP+JukLB7AaX4j5ZHh30TW+db/wGToWy9dRRJffGczbM8Kqu3kwCKRcH1X0NT4kfiN4f94l1E82ocx+uovbspoynAdZrPNcJEEqLEtinMIgk/XwMOKdsSUXwAMrASNVePyBPiuT9yMuR31fXoZSARpngrr0fPwusqZp8WPFBnyxdWwUkfyAsyJ1A16j5+nmLdq/axd7A3BTX60dG/bCK/KVxmAMEf1MzlgKL1U5synj1mwMUa2cIKmkQn6KWjcbm6vI4WJlRZ9GccOVbBoJNhr67iW3+V08x5k5NZJCdDrndqH+OtFmuJ6Y8EFku/Sy91DZXPiS/SnP/wCbCGPNlE27Fhs++bBPTwcEezPTC9o8y6BjqmAaDeZOOyRojK1rK7eLWUMkL0DRDMnIjObccbPDNqpENReWiO4UQ5xFAYOrMRZsBJ3YwTqUsP/s8pzsoGlsA7S5gvAg9LV2FG22RD0ruR9blVkwMjbrU8K3b7GohCh40ugVPoWvejOjgi9oixcEHzWIsls1k//9QJ/i+sToVYLThoSJWXKk4GE+8AOkvjaXya59kjf5PABae3FO2RmAxpZjzfS07Jx/U="

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
    def start_message(pool="default_pool"):  
        print("Initiating the CSV processing workflow.")  

    @task
    def search_personal_data(query):  
        return pd.DataFrame(search_zoom_info(query))          

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

    @task
    def consumer_kafka():
        # from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
        # from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
        print("Getting the msgs from kafka")

    def comsumer_sqs(queue_url, max_number_of_messages=1, wait_time_seconds=0):  
        # queue_url = 'https://sqs.YOUR_REGION.amazonaws.com/YOUR_ACCOUNT_ID/YOUR_QUEUE_NAME'  
        # consumer_sqs(queue_url=queue_url, max_number_of_messages=5, wait_time_seconds=10)
        session = boto3.Session()  
        sqs_client = session.client('sqs') 
        response = sqs_client.receive_message(  
            QueueUrl=queue_url,  
            MaxNumberOfMessages=max_number_of_messages,  # Can retrieve up to 10 messages  
            WaitTimeSeconds=wait_time_seconds  # Long polling can be set by specifying some wait time  
        )  
        messages = response.get('Messages', [])              
        if not messages:  
            print("No messages received.")  
            return None

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
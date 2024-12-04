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

aws_access_key="ASIA4RCAOLGLGOKM4NRL"
aws_secret_key="UZeXlfSTHkyZ92/PKyS2EqWs/vnpg+aHIjMyzk2V"
aws_session_token="IQoJb3JpZ2luX2VjEEUaCXVzLWVhc3QtMiJHMEUCIQCLV8guCr904/rj1dE4Fd+J4ZgGnGKni81oDhPqMx/I2AIgdAFq9Fn0RTapn5eg4d+7KmEopmZ0UhltnGy15yga8ukqowMI7v//////////ARAAGgw4NjEyNzYxMDEwMTQiDPb5twkmkyGMbLpxDSr3AuAjSeoqKdiRwZ1be9h2fXB9CnL63tE/zI+oluakjmC/sIPrh4wiBFKQVAw/A+IigWlec3M1/NAPEmhCwslAQwwSFyj+kfWLJvFpLV86koG2ry7CKrfZ/5RE/pNKRMCnK22iWzPYP+SOy7a5gC7iNFjUxUT4Irpc5QAxriGW+ammk17EZVEngHd9gtKXYj8xWeeQzxmTjh74A5aOa/1JOIyssqhJx0YE7kIjsZyUUumN2K+I9fkzqRkIz+YmhwG8gqGFnVrayp7kTYt8tGPxThpJ1lUEA9qouCrGxaf1eLg5V6oPITrUnlEtN7JXjHMLKJzgiF4Guq93BQyvVT6QHXnRZ1rRScNe8xUzN3cgHg9a19u37hZY802qj0ISF+WUmfzd7emegA4CWlHoyIYQD5IwcRSEVojxHCWlwnGLXksPvYUi6hCy/Cnkbv9jLlGOD0ehgxbOYK3twJYQpdKSUPqirgg25IrIaGi/zmkUMEaoUfk4SZIPkjCArsG6BjqmAbYDOks4b7/VAdqEUQhpzHpSF+Ry5zyqtDiwdgWsWzOru6AS2oxIDmYTLCuUyLSu3YJbSdvtlRMxG3k9z+hkr8EmxM+WwgfQNWQ6zOIjbxyWWEJdun31/cwmMmA7SyUIWQDoefLTLH9Far/+sNwzOW6p1gdemVy8FHeGvxenKKr5jl8Z1yTXx6p+3Ap5IpwGtoCKK/J+0uCzQANJvLNk50p4ezZaAIo="

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

    @task
    def fetch_file_keys():
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
        return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    consumer = consumer_kafka()
    file_keys = fetch_file_keys()

    # for i in range(len_file_keys):
    #     start = start_message.override(task_id=f"start_task_{i}")()

    #     df = fetch_file_from_s3.override(task_id=f"fecth_file_{i}")(file_keys[i])
    #     df_emails = process_email.override(task_id=f"valied_email_{i}")(df)
    #     profiles = process_profiles.override(task_id=f"process_profile_{i}")(df_emails)  
    #     activities = process_activities.override(task_id=f"process_activities_{i}")(df_emails)  
    #     companies = process_companies.override(task_id=f"process_companies_{i}")(df_emails)
    #     websites = process_websites.override(task_id=f"process_websites_{i}")(df_emails)  
    #     synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_keys[i], df_emails, profiles, activities, companies, websites)
    #     end = end_message.override(task_id=f"end_task_{i}")()
    start = start_message()
    df = fetch_file_from_s3.expand(file_key = file_keys)
    df_emails = process_email.expand(df = df)
    profiles = process_profiles.expand(df = df_emails)
    activities = process_activities.expand(df = df_emails)
    companies = process_companies.expand( df = df_emails)
    websites = process_websites.expand(df = df_emails)
    synthesis = synthesize_results.expand(file_key = file_keys, df_emails = df_emails, profiles = profiles, activities = activities, companies = companies, websites = websites)
    end = end_message()
    consumer >> file_keys>> start >> df >>  df_emails >> [ profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
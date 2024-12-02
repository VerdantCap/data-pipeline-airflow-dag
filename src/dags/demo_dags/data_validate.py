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

aws_access_key="ASIA4RCAOLGLFAGAH6MG"
aws_secret_key="kxijWCcXryzhJNCjxDv+lspowDKaJapMTNC+UMeX"
aws_session_token="IQoJb3JpZ2luX2VjEBoaCXVzLWVhc3QtMiJIMEYCIQCmsHkCPsa6Dmg8g4Mv/rOb3MImIvK/XIGxIC+5abBvIwIhAOGO6VGTAnkWweyT0JNQANH/cKv6sTDRvde55C63p0fCKqMDCMP//////////wEQABoMODYxMjc2MTAxMDE0Igwh36hApAN/ZrQZz6kq9wJ7V4nKybRjwcRXJ0naCxQIQQOFDCF42Rp0qwUfOn339TpH6FYpaqFsHtPYghlVCWaBLD8v9pesWpGkXl9Irz8kVIcrJXUCeiOWL54aQjvd/qHqDNJJErof3EeEp4PQHzKmxS+/vVK03ulV00GRROpj8Jpf5P9CC5WVdxwWbCk6HRX5aHjg9zbbWaFjF34G9IaEwa2WUr4j/DFCTk5JEdq7UfZ0psHOsmdacBr53xWoaT/+vFr0aLsXph7IyNoUaeb5bSv8EBEMvu9i0jGg3jgoZKIC5mAZYY3SxgUMimTLKX6lnP+IXSkLiu3WUB+Igt7k8ig/KjeI0R6vaar4FX0+IhIL9erRNEUsbWGUimC73p5J8lIQ0lBbwoGrz01S54+gWQP3rG/2QyZ4gfSc1+dL0OWIRufLzYSGEv5BR9xN3i11aLnxmo/okSdDai4f53MF3AI26DZqI7ZBG7fzWOYtsFMu8Y6MwW1+buCd33n99lnUpLBUajowuvG3ugY6pQHp5sbBHKhSMDOoEvX8l1e+Cti9+hvidvMClCsPQ7qvrbrwZ5Jh1uymBK2fDpurGBoiOaJ3fkfgG3kX6O5beQSh1IwLFVfh0hEbtocuUwMOzuaQ4xB1z5u+Spv/iq3Ddz6R4VODWOBdedO8iqAhoV1eQwWZfhbXbYG74vxmWmP5B3X3rwyELcWb7LpHb9S6X0mawKlcOAyguFwQP7QpJVzr7k00f3E="


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

    for file_key in file_keys:
        start = start_message.override(task_id=f"start_task_{file_key}")()  
        df = fetch_file_from_s3.override(task_id=f"fecth_file_{file_key}")(file_key)
        emails = process_profiles.override(task_id=f"valied_email_{file_key}")(df)
        profiles = process_profiles.override(task_id=f"process_profile_{file_key}")(df)  
        activities = process_activities.override(task_id=f"process_activities_{file_key}")(df)  
        companies = process_companies.override(task_id=f"process_companies_{file_key}")(df)
        websites = process_websites.override(task_id=f"process_websites_{file_key}")(df)  
        synthesis = synthesize_results(file_key, df, emails, profiles, activities, companies, websites)
        end = end_message.override(task_id=f"end_task_{file_key}")()
        start >> df >> [emails, profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
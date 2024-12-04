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

aws_access_key="ASIA4RCAOLGLCDCNTBLP"
aws_secret_key="wz0YErN9TInhqQVO96D06ueNFFagtW09Eh0yIfUt"
aws_session_token="IQoJb3JpZ2luX2VjEEcaCXVzLWVhc3QtMiJGMEQCIFTsj3N0kWv3OkfTj2DwuUlKwXeYEOUHt7Viqj2PiZSHAiATi9ZfTnCO/JtryudHV0gK9sbKO/ERh94+1mFRLUbV9yqjAwjw//////////8BEAAaDDg2MTI3NjEwMTAxNCIMj9KnzONz7mTRDCOHKvcCLkJyIrbNicWgZEWISzgOXZtoUC/7UzqIJZikBgroD9Nw8VaZIpWVyOHR88+9R0OvZvx597rqw088waPtxocCnufe1eyDLFyJk7e0akg8/CkUvJIIZLSQH8d0jCX31cr8mJNkVSmKY3IuzPRA5tXjRdW/UfLEg27K9vlScib7uvANbjHdY3qYQv019KyOeYwLGsmvv+N87epmU/4Ppl3y7XmiHKATp/ysQ7CVF0m68NsdcRVS9HrUFKHC4VZTECN2TFs7oksK/nPgU7xwt5ROaDAWntVYbmbtHtrWk0Gz9F2zcO8XSoPZTAsqc6uox7qOy22fPGgve0W9EZh8ztdd8lhaIkrTxbapKgbCE+HRhiKRsZRupjHUuWqOVIMIJ/4VoKq+UqvGK8KKvampcrNr8nnQ6PT9Hkf6vUWPezcKtiVYnhQM1kiqvBV2znnMiU5w/kQqMAUsdT+GCOXCtTwqk3WUKhYWTYtYsqoKMjQE+Zr+FTxaKfJpMN7awboGOqcBUr63kDDrRUOmeN71yXQP+IjSrUR1rlbAsgj7YvklE2tEiZ4RstFqpSWlvfHjMmX6byuScA5fMdWLiduQ7mRKs0X5jKRGUq65yihdAObUaUCCrezCirn7P/Nqr+U8CnQOBcsAYsdx+BQVtR/phkGwNZHhB66nwp+6vvPnJl8KjnaXNzouJI1hSgeeFAjey3vFyPEfqN2Ryve6ZoKLjCPgQin61vmwgz4="

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
    def fetch_file_keys():
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
        return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]



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

    file_keys = fetch_file_keys()
    df = fetch_file_from_s3.expand(file_key = file_keys)
    df_emails = process_email.expand(df = df)
    profiles = process_profiles.expand(df = df_emails)
    activities = process_activities.expand(df = df_emails)
    companies = process_companies.expand( df = df_emails)
    websites = process_websites.expand(df = df_emails)
    synthesis = synthesize_results.expand(file_key = file_keys, df_emails = df_emails, profiles = profiles, activities = activities, companies = companies, websites = websites)
    
    file_keys >> df >>  df_emails >> [ profiles, activities, companies, websites] >> synthesis

dag_instance = process_csv_files()  
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

aws_access_key="ASIA4RCAOLGLFZGZTRN4"
aws_secret_key="mt1cY0gCC8l0TeNi1EleMvFbx6uamz5G4WHKXn15"
aws_session_token="IQoJb3JpZ2luX2VjEG8aCXVzLWVhc3QtMiJHMEUCICRSCXSakBstM6Rft6r7D28D8/d5MW85EHyFP9sDTEI5AiEAlYmPVMXnbP0sryxZvJxeMX4XNtL9aee7ftEAyzq2Cb8qmgMIKBAAGgw4NjEyNzYxMDEwMTQiDA+iQuNDq77Kqj9iSCr3AukyOnJEPObCj4JSiyL2nbsNc/XgeN/NydK8k8bwN09lw0EXA5EPVmVfUpWavykCc0YogGSxZeY1vkEGp7klBdqeJCJY0xDnuW9OsBhg7qLuSGIACBTdd5Vgjloh0hE2m7NhhrWU2/Af7wTkCMEe/e/C511ZQWhXrsWRQAJpQNtKETfAoaLhD3qKXnBY/qNf99z7wSgJ/eGNA6VRJIjGvIE1uYoo+eGGPAx6Hl7q+G3UHjaeV3ClBLicvDf6v1SDQmxK8sLZLDKs7Ey9ZoRarp7WJW58w0RxZInVMPxGVh0mPEZMYNcMVTkRrAprJ641JaMgBOQnN75n0DZFRO2cC7d7CShBy4IuMehRif6zXC73VWlua/P+aeTXWwvkQLc1ARaGTZJqI12HUls8eJwJTonu4qS3I8lnkefd4oGEdxKpaANJHpFmaAtDX6Ga/CE0yedG8cNv7O8r1Uh6jrca33EEB8MnqBzmUvSKkkMHNRuntEBKyeFwTTDnxsq6BjqmAXIc5fVfTSxmym86sqIzTg2u5A9S2clYeWzmHPBWIZ7smwLsNgaJgV2IQu119GOesDZb/dGX2UkMt0H7Hk2fiCDLV2odmJ+oNpsqNLrAZ2m2XpNs706glfv31euuKChXz9YUo/L1MXCo+GP/HxZvWhzj/OQzoN6UJVtKmsQSGW257F9E9evAloO/fGcWtvwwYhCoQT/3quyC+wHxxTa73Hnc6MD2SW0="

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
        file_keys =  [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
        return file_keys
    @task
    def processing_files(file_keys):
        for i, file_key in enumerate(file_keys):            
            df = fetch_file_from_s3.override(task_id=f"fecth_file_{i}")(file_key)
            df_emails = process_email.override(task_id=f"valied_email_{i}")(df)
            profiles = process_profiles.override(task_id=f"process_profile_{i}")(df_emails)  
            activities = process_activities.override(task_id=f"process_activities_{i}")(df_emails)  
            companies = process_companies.override(task_id=f"process_companies_{i}")(df_emails)
            websites = process_websites.override(task_id=f"process_websites_{i}")(df_emails)  
            synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_key, df_emails, profiles, activities, companies, websites)
            
            df >>  df_emails >> [ profiles, activities, companies, websites] >> synthesis
    start = start_message()
    file_keys = fetch_file_keys()
    process_files = processing_files(file_keys)
    end = end_message()
    start >> file_keys >> process_files >> end

    

dag_instance = process_csv_files()  
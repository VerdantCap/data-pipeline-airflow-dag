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

aws_access_key="ASIA4RCAOLGLEQLZNT4Z"
aws_secret_key="QzM23YGrXUy3lbhnT1xOaPFCaMFSoEo+cXGG5k26"
aws_session_token="IQoJb3JpZ2luX2VjEDIaCXVzLWVhc3QtMiJIMEYCIQD84QRCSMZ8jy+3CNrKc0xOrBuvK0Qif34nf5zgi922/QIhAJckiRg88YOjNySZE1aHq4DMmE9++w/RRNVRrsol4G00KqMDCNv//////////wEQABoMODYxMjc2MTAxMDE0Igya+4e0ZRlvSXK6Cxoq9wKnFD7gClzy3LC7ocOeeBNWKEqnDkdsjuEdAie0hOLQFAimcmJ0Auft+txo640EUyCOG/DjgQMyYnXnbNvbzKL/Xijp7OgS88UmuBe4xkG6TRHTj0TyVRbXBMsQYfzw9rzJNphj8ihkqZUQ90ZA63yiZfySxHQFlWn4U25J/nKUVEXttl6K/F5IC1X07GwJpq0K97mjHDGT/d371xVSkybV0Dx5sNccon8FnSozX4khHOjr2RCKaynAxWpapJsuYgfH5QPll4exXbHB2g9ggc2pI5cP+P8Yjcm1MLF9cMUUZ+QyUNwHLqb0klg2FIxkM5P4CtW42rUEimunOgWrprRcoNRJsfgKgh+sbJAoNPYG6qdWUwitn6w9LyzzMWU3KdUeec6KfVvZCeKTbkG5saMzfWHPOg+sVhMfiyHcyO7P4k2HL60b+Rc4bQ3bZ//s7Puzx5+ae2pjauOy8sCKpMnbOReVhI54ZXIS61LSKCZbqWMfa+Q8JcEwgIG9ugY6pQHA1lBT8xgIDbLLoT8KeYHgkJoDY6MOjG2N3DdLqwuSCxyU6OuwceK3ZqHtnVFwd0cncm/DwZojimyzvlZLhVOkKaaYcgTiJBbo3H3YKWntBqnoGeLS9f1GsW8Pa3kutU/ZZbkRp0Bq+0+MyfvaXMZkh0d5VV+KuXv2ac/Y0Cmyj6LnpBbS7+8HsuGhZulOc+T6W86C29SA0+47YX6m1KbVaxp1gTI="


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
        df = df.dropna(subset=["Email Address"])
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
        emails = process_email.override(task_id=f"valied_email_{i}")(df)
        profiles = process_profiles.override(task_id=f"process_profile_{i}")(df)  
        activities = process_activities.override(task_id=f"process_activities_{i}")(df)  
        companies = process_companies.override(task_id=f"process_companies_{i}")(df)
        websites = process_websites.override(task_id=f"process_websites_{i}")(df)  
        synthesis = synthesize_results.override(task_id=f"synthesis_task_{i}")(file_key, df, emails, profiles, activities, companies, websites)
        end = end_message.override(task_id=f"end_task_{i}")()
        start >> df >> [emails, profiles, activities, companies, websites] >> synthesis >> end 

dag_instance = process_csv_files()  
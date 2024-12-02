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

aws_access_key="ASIA4RCAOLGLB6E2NRP5"
aws_secret_key="Pp5dIzfpb6zWmekX51KnuvvrHen9rxeizrsWlRpD"
aws_session_token="IQoJb3JpZ2luX2VjEB0aCXVzLWVhc3QtMiJIMEYCIQCtkn0gAeveuDTcAG0vPdBh8UrEIF0farwAsgM6dr+t0AIhAJjNx//x6uWZa2sBQs/cyb84SFP74VSgPlaAwTe95xh9KqMDCMb//////////wEQABoMODYxMjc2MTAxMDE0IgxdWG2A1ENzLtDf5Rwq9wLW6vLThzs/DikRwdh4SqPCmPPyRljzPV41l+powOWo39fS9BHVOcmCuJk1cecAIKdf+v+vH9OO5Culnvl4AS649dqPYrxTeKU7bzgzbYozn8yDbklJDk+CBDYGVidn3VEXZFZUxyvluqfUVPA1BM2Vo2l7SKBmrYiCc2/Csh1n97yECEvLNB4cNZsUmtXII6NEjEx+koXcnqpbLiNggAHEKKY/VpW8bIPugaZrYKOMtuT++9e2NBjyBJNyYQbEAOFA5FnYp0Md3W6eS0b7UQCQ74kPqB6fkINsxkm3ZhYpV64N2/A5PM01sJ3Nj0i5fLQzN/j2yfVpOBTexBmvKSvEnx3/0ZFOqdVomS2iqI3pvb0UaAZnzHOuWNfQwTYrEKJPKNUB4zbWKdz/FQESOUKQBRYOgZj1BbicvZ7mGKFhYA3p559Topd53IAjFGmK6QG12a9/vfOB9ZHKFvcjlvs+Il2aBcF6c3ONvxquY24p9yYulpPKuhgwt7i4ugY6pQFuvUhcEbtG8CvI8GdNxHQTGOXog8+ppTX/iqSJFma9aaiB4Q25NPDARiVjmKKp4evzD1kdpBLloyDuEQr7Lkair9js6PGubVvCSXmRCNwbxOk+No/LAGSoro4r2ZDWgHtTMFwBJB4FvY5Ep06sekLl4Yi+LOpnIu8PQq2r+vSPWRyEDZLCEJfvEdG1hwhlX1dutO6VF/QtbD6gl3nPGATrS++5TgM="


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
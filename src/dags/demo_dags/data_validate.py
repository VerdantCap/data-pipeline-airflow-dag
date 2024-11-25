from datetime import datetime, timedelta  
from airflow.decorators import dag, task  
import boto3  
import pandas as pd  
import os  
from airflow.models import Variable  

DAG_ID = "Process_CSV_Files"  
S3_BUCKET_NAME = "airbyte-state-dev-us-east-2-genie-platforms"  
S3_DIRECTORY = "air-byte-sync-destination/zoominfo-preview/"  
OUTPUT_DIRECTORY = "air-byte-sync-destination/zoomiinfo-validate/"  

aws_access_key = Variable.get("AWS_ACCESS_KEY", default_var="your_default_access_key")  
aws_secret_key = Variable.get("AWS_SECRET_KEY", default_var="your_default_secret_key")  
 
@dag(  
    dag_id=DAG_ID,  
    schedule_interval="* * * * *",  # Run every minute  
    start_date=datetime(2024, 11, 25),  
    dagrun_timeout=timedelta(minutes=5),  
    catchup=False,  
    is_paused_upon_creation=False,  
)  
def process_csv_files():  

    @task  
    def fetch_files_from_s3() -> list:  
        """Retrieve CSV file keys from a designated S3 directory."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)  
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
        
        file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]  
        return file_keys  

    @task  
    def process_file(file_key: str):  
        """Download, cleanse, and upload CSV file data back to S3."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)  
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)  
        data = pd.read_csv(response['Body'])  

        # Data cleansing (example: removing NaNs)  
        data_cleaned = data.dropna()  

        # Writing back to S3  
        output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"  
        csv_buffer = data_cleaned.to_csv(index=False).encode()  
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer)  

        return f"Processed and saved: {output_key}"  

    @task  
    def start_message():  
        print("Initiating the CSV processing workflow.")  

    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    start = start_message()  

    file_keys = fetch_files_from_s3()  

    # Airflow's looping with task instances for each file  
    for file_key in file_keys.output:  
        p = process_file(file_key)  
        start >> p >> end_message()  

dag_instance = process_csv_files()  
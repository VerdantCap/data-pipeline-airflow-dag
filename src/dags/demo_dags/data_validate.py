from datetime import datetime, timedelta  
from airflow.decorators import dag, task  
import boto3  
import pandas as pd  
import os  
from airflow.models import Variable  

DAG_ID = "Validate_CSV_Files"  
S3_BUCKET_NAME = "airbyte-state-dev-us-east-2-genie-platforms"  
S3_DIRECTORY = "air-byte-sync-destination/zoominfo-preview/"  
OUTPUT_DIRECTORY = "air-byte-sync-destination/zoomiinfo-validate/"  

# aws_access_key = Variable.get("AWS_ACCESS_KEY", default_var="your_default_access_key")  
# aws_secret_key = Variable.get("AWS_SECRET_KEY", default_var="your_default_secret_key")  
# aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="you_default_secret_key")

aws_access_key="ASIA4RCAOLGLLU4GNDDB"
aws_secret_key ="Klxg9CA7Hn+imZwleNvJqN+BNNYRMG3AF7WoWa3S"
aws_session_token="IQoJb3JpZ2luX2VjEIf//////////wEaCXVzLWVhc3QtMiJGMEQCIH00hXzkwg1q3pMjU/hisxElMvzEthyd7KLbeurB18J0AiBls6MIQ03yam0azWnVzLgr4WJWpdQ8S2vD+qgcc1VuLCqaAwgwEAAaDDg2MTI3NjEwMTAxNCIM1aW67qRWBI6CwknOKvcCIsBeL8QKuWySa8Z3zY40IYe123Ytyn1gS6VgauwnH+eyPTURUiwdlcFhMXQ9PE6UyUFPhO7seM2QLqiJYr2Dxo+NJjjYjguiDgbF+LQ2HqRlro6w1OYkmhYC4T7DGDfw6FmSS7pcVkg791zZsEu06RcZ3H4tpMOhUqJ0VRYtJnrNshw9teHNHO8YeqZDFVFe2P4emCzgDalWI5cjLW8SBtIFSvKsa0wot79Veleur9LcaPIyPf2X4NvGYsGVlfyQCc+8gBFkhTUsHf49pTbAY3CcOnxVn0Ou61RPq6sO4Kuugps501DjK2LvWbzYQ4YC/dn24320eB7xYYtSvExosmifvIwuK7oPck8TFv4Rq5WSmKg30gVkBEzXYNdnBO4uLUhk8HZkwKupvxCun22p1IkpxcepcuUkftGbpKS3LAjni0YcI4vv4bF8gjYg5YEiQ4Dwc3qKwhb1EbZ5bhwD8L2mQ3xGScf7wW8fKHCEAGpFbLSGZJnKMLe7l7oGOqcBNlOcEXk7UEhgAqwJmAqgGEI3cLLrPl+3MvI1abJeuD8GfxXxCbsHv0iec88zx133XG7qhBhAGwxSunT7XYjAjmuMfalxL1T67goeAhSldojX1+Zd6XDrRalY2PYfzwnhNNl3Mg4fSpITqdogzAdW++pz/Sq0Ao75TEZ21bMN4Q5wetHzS9KsuW2XDH4l15oLSYIEaNiJH1axnZ1J9Gkqv7CbSHCG3MM="
 
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
        """Retrieve CSV file keys from the designated S3 directory."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_DIRECTORY)  
        file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]  
        return file_keys  

    @task  
    def process_files(file_keys: list):  
        """Download, process, and upload each CSV file."""  
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, aws_session_token=aws_session_token)  
        
        for file_key in file_keys:  
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)  
            data = pd.read_csv(response['Body'])  

            # Example: Data cleansing  
            data_cleaned = data.dropna()  

            # Save cleaned data back to S3  
            output_key = f"{OUTPUT_DIRECTORY}{os.path.basename(file_key)}"  
            csv_buffer = data_cleaned.to_csv(index=False)  
            s3.put_object(Bucket=S3_BUCKET_NAME, Key=output_key, Body=csv_buffer.encode('utf-8'))  

    @task  
    def start_message():  
        print("Initiating the CSV processing workflow.")  

    @task  
    def end_message():  
        print("CSV processing workflow completed.")  

    start = start_message()  
    file_keys = fetch_files_from_s3()  
    process_files(file_keys)  
    end = end_message()  

    start >> file_keys >> process_files(file_keys) >> end  

dag_instance = process_csv_files()  
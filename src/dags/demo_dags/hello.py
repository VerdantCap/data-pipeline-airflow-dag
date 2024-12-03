from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import boto3
from botocore.exceptions import NoCredentialsError

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def list_s3_files(bucket_name: str, prefix: str = ''):  
    """  
    List all files in the specified S3 bucket and subdirectory (prefix).  

    :param bucket_name: Name of the S3 bucket.  
    :param prefix: S3 object key prefix to filter files within a 'subdirectory'.  
    """  
    # Create a session using the default configuration  
    # Boto3 will automatically use the credentials provided by IRSA  
    session = boto3.Session()  
    s3_client = session.client('s3')  
    
    paginator = s3_client.get_paginator('list_objects_v2')  
    
    try:  
        # Set the prefix to look within a specific 'folder'  
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):  
            for obj in page.get('Contents', []):  
                print(obj['Key'])  
    except NoCredentialsError:  
        print("Credentials not available.") 

# Instantiate the DAG
dag = DAG(
    'hello_world',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(days=1),
)

# Define the Python function to be executed
def print_hello():
    bucket_name = "airflow-storage-dev-us-east-2-genie-platforms"
    prefix = 'airflow/logs/dag_id=hello_world'  # Change this to your subdirectory  
    # List files in the S3 bucket subdirectory  
    list_s3_files(bucket_name, prefix)


# Create tasks
start_task = EmptyOperator(task_id='start_task', dag=dag)
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)
end_task = EmptyOperator(task_id='end_task', dag=dag)

# Set task dependencies
start_task >> hello_task >> end_task
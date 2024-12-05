# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.providers.apache.kafka.operator.consume import ConsumeFromTopicOperator
# def func(message, prefix=None):
#     print(message)

# with DAG(dag_id="test_kafka",
#          start_date=datetime(2021,1,1),
#          schedule_interval='*/2 * * * *'
#          ) as dag:
#          get_messages=ConsumeFromTopicOperator(
#           task_id="get_messages",
#           topics=["topictest"],
#           apply_function='test_kafka.func',
#           consumer_config = {
#             'group.id':'test-consumer-group',
#             'bootstrap.servers': 'server:9092',
#             "auto.offset.reset": "earliest",})

# get_messages

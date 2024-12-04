from __future__ import annotations

import functools
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
import logging
import os
from typing import TYPE_CHECKING, Callable

from tabulate import tabulate

from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_connections():
    # Connections needed for this example dag to finish
    from airflow.models import Connection
    from airflow.utils import db

    db.merge_conn(
        Connection(
            conn_id="t1-3",
            conn_type="kafka",
            extra=json.dumps({"socket.timeout.ms": 10, "bootstrap.servers": "broker:29092"}),
        )
    )

    db.merge_conn(
        Connection(
            conn_id="t2",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "broker:29092",
                    "group.id": "t2",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )

    db.merge_conn(
        Connection(
            conn_id="t4",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "broker:29092",
                    "group.id": "t4",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )

    db.merge_conn(
        Connection(
            conn_id="t4b",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "broker:29092",
                    "group.id": "t4b",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )

    db.merge_conn(
        Connection(
            conn_id="t5",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "broker:29092",
                    "group.id": "t5",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )


def producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))


consumer_logger = logging.getLogger("airflow")


def consumer_function(message, prefix=None):
    key = json.loads(message.key())
    value = json.loads(message.value())
    consumer_logger.info("%s %s @ %s; %s : %s", prefix, message.topic(), message.offset(), key, value)
    return


def consumer_function_batch(messages, prefix=None):
    for message in messages:
        key = json.loads(message.key())
        value = json.loads(message.value())
        consumer_logger.info("%s %s @ %s; %s : %s", prefix, message.topic(), message.offset(), key, value)
    return


def await_function(message):
    if json.loads(message.value()) % 5 == 0:
        return f" Got the following message: {json.loads(message.value())}"


def hello_kafka():
    print("Hello Kafka !")
    return

with DAG(
    "kafka-example",
    default_args=default_args,
    description="Examples of Kafka Operators",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    t0 = PythonOperator(task_id="load_connections", python_callable=load_connections)

    # [START howto_operator_produce_to_topic]
    t1 = ProduceToTopicOperator(
        kafka_config_id="t1-3",
        task_id="produce_to_topic",
        topic="test_1",
        producer_function="example_dag_hello_kafka.producer_function",
    )
    # [END howto_operator_produce_to_topic]

    t1.doc_md = "Takes a series of messages from a generator function and publishes"
    "them to the `test_1` topic of our kafka cluster."

    # [START howto_operator_consume_from_topic]
    t2 = ConsumeFromTopicOperator(
        kafka_config_id="t2",
        task_id="consume_from_topic",
        topics=["test_1"],
        apply_function="example_dag_hello_kafka.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )
    # [END howto_operator_consume_from_topic]

    t2.doc_md = "Reads a series of messages from the `test_1` topic, and processes"
    "them with a consumer function with a keyword argument."

    t3 = ProduceToTopicOperator(
        kafka_config_id="t1-3",
        task_id="produce_to_topic_2",
        topic="test_1",
        producer_function=producer_function,
    )

    t3.doc_md = "Does the same thing as the t1 task, but passes the callable directly"
    "instead of using the string notation."

    t4 = ConsumeFromTopicOperator(
        kafka_config_id="t4",
        task_id="consume_from_topic_2",
        topics=["test_1"],
        apply_function=functools.partial(consumer_function, prefix="consumed:::"),
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )

    t4b = ConsumeFromTopicOperator(
        kafka_config_id="t4b",
        task_id="consume_from_topic_2_b",
        topics=["test_1"],
        apply_function_batch=functools.partial(consumer_function_batch, prefix="consumed:::"),
        commit_cadence="end_of_batch",
        max_messages=30,
        max_batch_size=10,
    )

    t4.doc_md = "Does the same thing as the t2 task, but passes the callable directly"
    "instead of using the string notation."

    # [START howto_sensor_await_message]
    t5 = AwaitMessageSensor(
        kafka_config_id="t5",
        task_id="awaiting_message",
        topics=["test_1"],
        apply_function="example_dag_hello_kafka.await_function",
        xcom_push_key="retrieved_message",
    )
    # [END howto_sensor_await_message]

    t5.doc_md = "A deferrable task. Reads the topic `test_1` until a message with a value"
    "divisible by 5 is encountered."

    t6 = PythonOperator(task_id="hello_kafka", python_callable=hello_kafka)

    t6.doc_md = "The task that is executed after the deferrable task returns for execution."

    t0 >> t1 >> t2
    t0 >> t3 >> [t4, t4b] >> t5 >> t6


def get_test_run(dag):
    def callback(context: Context):
        ti = context["dag_run"].get_task_instances()
        if not ti:
            logger.warning("could not retrieve tasks that ran in the DAG, cannot display a summary")
            return

        ti.sort(key=lambda x: x.end_date)

        headers = ["Task ID", "Status"]
        results = []
        for t in ti:
            results.append([t.task_id, t.state])

        logger.info("EXECUTION SUMMARY:\n%s", tabulate(results, headers=headers, tablefmt="fancy_grid"))

    def add_callback(current: list[Callable] | Callable | None, new: Callable) -> list[Callable] | Callable:
        if not current:
            return new
        elif isinstance(current, list):
            current.append(new)
            return current
        else:
            return [current, new]

    def test_run():
        dag.on_failure_callback = add_callback(dag.on_failure_callback, callback)
        dag.on_success_callback = add_callback(dag.on_success_callback, callback)
        # If the env variable ``_AIRFLOW__SYSTEM_TEST_USE_EXECUTOR`` is set, then use an executor to run the
        # DAG
        dag_run = dag.test(use_executor=os.environ.get("_AIRFLOW__SYSTEM_TEST_USE_EXECUTOR") == "1")
        assert (
            dag_run.state == DagRunState.SUCCESS
        ), "The system test failed, please look at the logs to find out the underlying failed task(s)"

    return test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
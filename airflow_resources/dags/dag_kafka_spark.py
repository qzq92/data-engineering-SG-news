from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from src.kafka_client.kafka_stream_data import stream
from src.constants import URL_TOPIC, SPARK_PKG
# Define start day as 1 day before date of execution
start_date = datetime.today() - timedelta(days=1)

# Define arguments for DAG: Concerned with how to execute them
default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,  # number of retries before failing the task
    "retry_delay": timedelta(seconds=5),
}

# Define DAG as context manager
with DAG(
    dag_id="kafka_spark_dag",
    default_args=default_args, # Contains other key value pairs
    schedule_interval=timedelta(days=1), #Alternative can be schedule="@daily"
    catchup=False,
) as dag:

    # Define DAG tasks
    # Python task
    kafka_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=stream, # python function
        op_kwargs = None,
        dag=dag,
    )

    # Docker task
    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image=f"{URL_TOPIC}/spark:latest", # Our docker image to run
        api_version="auto",
        auto_remove=True, # Remove after completion
        command=f"./bin/spark-submit --master local[*] --packages {SPARK_PKG} ./spark_streaming.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'}, # Docker image environment
        network_mode="airflow-kafka",
        dag=dag,
    )

    # Dependency order
    kafka_stream_task >> spark_stream_task
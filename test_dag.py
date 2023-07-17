from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.rawlayer import transform_and_output_to_hdfs
from src.cleansed import transform_and_save_to_hive_hdfs
from src.curated import sales_analysis
from kafka_hdfs_streaming.pyspark_streaming_kafka import process_csv_to_hdfs

default_args = {
    'start_date': datetime(2023, 7, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')

streaming_kafka_task = PythonOperator(
    task_id='streaming_kafka',
    python_callable=process_csv_to_hdfs,
    dag=dag
)

raw_task = PythonOperator(
    task_id='raw',
    python_callable=transform_and_output_to_hdfs,
    dag=dag
)

cleansed_task = PythonOperator(
    task_id='cleansed',
    python_callable=transform_and_save_to_hive_hdfs,
    dag=dag
)

curated_task = PythonOperator(
    task_id='curated',
    python_callable=sales_analysis,
    dag=dag
)

streaming_kafka_task >> raw_task
raw_task >> cleansed_task
cleansed_task >> curated_task

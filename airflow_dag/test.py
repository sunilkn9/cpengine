from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_python_file():
    # Replace with the path to your Python file
    exec(open(r"C:\Users\sunil.kn\PycharmProjects\pythonProject\cleansed.py").read())

# Define the DAG
dag = DAG(
    dag_id='run_python_file_dag',
    description='DAG to run a Python file',
    schedule=None,  # Set the schedule as per your requirements
    start_date=datetime(2023, 7, 11),  # Set the start date as per your requirements
)

# Define the PythonOperator to execute the Python file
run_python_task = PythonOperator(
    task_id='run_python_file',
    python_callable=run_python_file,
    dag=dag,
)

# Set the task dependencies
run_python_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def simple_task():
    print("✅ Тестовый DAG работает!")
    return "Успех"

with DAG(
    'test_simple_dag',
    start_date=datetime(2024, 9, 30),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id='test_task',
        python_callable=simple_task
    )
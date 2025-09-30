from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import boto3
import os
from botocore.client import Config


# Загрузка JSON файлов в MinIO.
def load_all_to_minio():
    try:
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            config=Config(signature_version='s3v4'),
        )

        # Создание bucket.
        try:
            s3.create_bucket(Bucket='user-data')
        except:
            pass

        data_dir = '/opt/airflow/generated_data'

        # Загружаем ВСЕ найденные папки с датами.
        for item in os.listdir(data_dir):
            item_path = os.path.join(data_dir, item)
            if os.path.isdir(item_path):
                date_str = item

                # Загружаем пользователей.
                users_file = os.path.join(item_path, 'user_profiles.json')
                if os.path.exists(users_file):
                    s3.upload_file(
                        users_file,
                        'user-data',
                        f'raw/{date_str}/user_profiles.json'
                    )
                    print(f"✅ Пользователи за {date_str}")

                # Загружаем события.
                events_file = os.path.join(item_path, 'user_behavior_events.json')
                if os.path.exists(events_file):
                    s3.upload_file(
                        events_file,
                        'user-data',
                        f'raw/{date_str}/user_behavior_events.json'
                    )
                    print(f"✅ События за {date_str}")

        return "ВСЕ данные загружены в MinIO"

    except Exception as e:
        return f"Ошибка: {str(e)}"

with DAG(
    'load_one_time_to_minio',
    start_date=datetime(2024, 12, 25),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_all_data',
        python_callable=load_all_to_minio
    )

    check_task = BashOperator(
        task_id='check_generated_data',
        bash_command='echo "Проверяем данные..." && ls -la /opt/airflow/generated_data/'
    )

    check_task >> load_task
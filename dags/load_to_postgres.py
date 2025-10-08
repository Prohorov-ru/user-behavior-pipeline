from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from minio import Minio
import json
import io


# Загрузка данных из MinIO в PostgreSQL.
def load_minio_to_postgres():
    # Подключение к MinIO.
    minio_client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    # Подключение к PostgreSQL.
    conn = psycopg2.connect(
        host="postgres-data",
        database="user_behavior",
        user="data_user",
        password="data_password",
        port="5432"
    )

    try:
        # Получаем список объектов в MinIO.
        objects = minio_client.list_objects("user-data", prefix="raw/", recursive=True)

        for obj in objects:
            if obj.object_name.endswith('.json'):
                print(f"📥 Обрабатываем: {obj.object_name}")

                # Читаем файл из MinIO.
                response = minio_client.get_object("user-data", obj.object_name)
                data = json.loads(response.read().decode('utf-8'))
                response.close()
                response.release_conn()

                # Определяем тип данных и загружаем в соответствующую таблицу.
                if 'user_profiles.json' in obj.object_name:
                    load_user_profiles(data, conn)
                elif 'user_behavior_events.json' in obj.object_name:
                    load_user_events(data, conn)

        print("✅ Все данные загружены в PostgreSQL")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        conn.close()


# Загрузка профилей пользователей.
def load_user_profiles(data, conn):
    cur = conn.cursor()

    for user in data:
        cur.execute("""
            INSERT INTO user_profiles 
            (user_id, name, email, city, phone, registration_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            city = EXCLUDED.city,
            phone = EXCLUDED.phone
        """, (
            user['user_id'],
            user['name'],
            user['email'],
            user['city'],
            user['phone'],
            user['registration_date']
        ))

    conn.commit()
    cur.close()
    print(f"✅ Загружено {len(data)} пользователей")


# Загрузка событий пользователей.
def load_user_events(data, conn):
    cur = conn.cursor()

    for event in data:
        cur.execute("""
            INSERT INTO user_events 
            (user_id, event_type, event_timestamp, device, session_id, page_url)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            event['user_id'],
            event['event_type'],
            event['timestamp'],
            event['device'],
            event['session_id'],
            event.get('page_url')
        ))

    conn.commit()
    cur.close()
    print(f"✅ Загружено {len(data)} событий")


with DAG(
        'load_to_postgres',
        start_date=datetime(2024, 9, 30),
        schedule_interval=None,
        catchup=False,
) as dag:
    load_task = PythonOperator(
        task_id='load_minio_to_postgres',
        python_callable=load_minio_to_postgres
    )
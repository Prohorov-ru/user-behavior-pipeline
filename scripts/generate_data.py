# Генератор синтетических данных поведения пользователей.
# Генерирует профили пользователей и события активности.

from faker import Faker
import json
import os
import random
import sys
from datetime import datetime, timedelta

# Функция проверяет и создает необходимые папки для данных.
def setup_environment():
    try:
        data_path = '/opt/airflow/generated_data'   # Путь к папке generated_data в контейнере Airflow.

        os.makedirs(data_path, exist_ok=True)   # Создаем основную папку если не существует.

        timestamp = datetime.now().strftime("%d-%m-%Y")   # Создаем подпапку с текущей датой.
        timestamp_folder = os.path.join(data_path, timestamp)
        os.makedirs(timestamp_folder, exist_ok=True)

        print(f"📁 Данные будут сохранены в: {timestamp_folder}")
        return timestamp_folder

    except Exception as e:
        print(f"❌ Ошибка создания папок: {e}")
        print("💡 Запускайте скрипт внутри Airflow контейнера:")
        print("   docker-compose exec airflow-webserver python /opt/airflow/scripts/generate_data.py")
        sys.exit(1)


# Функция генерирует профили пользователей.
def generate_user_profiles(num_users=150):
    fake = Faker("ru_RU")
    users = []

    for i in range(1, num_users + 1):
        user = {
            'user_id': f'user_{i:03d}',  # user_001, user_002, ...
            'name': fake.name(),
            'email': fake.email(),
            'city': fake.city(),
            'phone': fake.phone_number(),
            'registration_date': fake.date_this_year().isoformat()
        }
        users.append(user)

    print(f"👥 Сгенерировано профилей пользователей: {len(users)}")
    return users


# Функция генерирует события поведения пользователей.
def generate_user_behavior(num_events=1000, num_users=150):
    fake = Faker("ru_RU")
    event_types = ['session_start', 'click', 'page_view', 'purchase', 'logout']
    devices = ['desktop_windows', 'macos', 'android', 'ios']
    user_ids = [f'user_{i:03d}' for i in range(1, num_users + 1)]

    events = []
    for _ in range(num_events):
        event = {
            'timestamp': (datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': random.choice(event_types),
            'user_id': random.choice(user_ids),
            'device': random.choice(devices),
            'session_id': f"sess_{random.randint(1000, 9999)}",
            'page_url': fake.uri() if random.random() > 0.3 else None
        }
        events.append(event)

    print(f"📊 Сгенерировано событий: {len(events)}")
    return events


# Сохранение данных в JSON файл.
def save_data(data, filename, folder_path):
    try:
        filepath = os.path.join(folder_path, filename)
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"💾 Сохранено: {filename}")
        return filepath
    except Exception as e:
        print(f"❌ Ошибка сохранения {filename}: {e}")
        return None


def main():
    # Основная функция генерации данных.
    print("🚀 Запуск генерации синтетических данных...")

    # Настройка окружения.
    output_folder = setup_environment()

    # Генерация данных.
    users = generate_user_profiles(150)
    events = generate_user_behavior(1000, 150)

    # Сохранение данных.
    save_data(users, 'user_profiles.json', output_folder)
    save_data(events, 'user_behavior_events.json', output_folder)

    # Статистика.
    print("\n📈 Статистика генерации:")
    print(f"   • Пользователей: {len(users)}")
    print(f"   • Событий: {len(events)}")
    print(f"   • Период событий: последние 30 дней")
    print(f"   • Папка с данными: {output_folder}")
    print("\n✅ Генерация данных завершена успешно!")


if __name__ == "__main__":
    main()
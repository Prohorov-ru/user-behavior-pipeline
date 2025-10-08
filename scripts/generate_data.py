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
        data_path = '/opt/airflow/generated_data'  # Путь к папке generated_data в контейнере Airflow.

        os.makedirs(data_path, exist_ok=True)  # Создаем основную папку если не существует.

        # Создаем папки за последние 7 дней.
        for i in range(7):
            date = datetime.now() - timedelta(days=i)
            timestamp = date.strftime("%d-%m-%Y")
            timestamp_folder = os.path.join(data_path, timestamp)
            os.makedirs(timestamp_folder, exist_ok=True)
            print(f"📁 Создана папка: {timestamp}")

        print(f"📁 Папки созданы за последние 7 дней")
        return data_path

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
        # Случайная дата за последние 7 дней.
        days_ago = random.randint(0, 6)
        event_date = datetime.now() - timedelta(days=days_ago)

        event = {
            'timestamp': (event_date - timedelta(
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


# Сохранение данных в JSON файлы по дням.
def save_data_to_multiple_days(data, filename, base_folder):
    """Сохраняет данные в папки соответствующих дат"""
    try:
        # Для пользователей (без timestamp) сохраняем во все папки.
        if 'timestamp' not in data[0]:
            for folder in os.listdir(base_folder):
                folder_path = os.path.join(base_folder, folder)
                if os.path.isdir(folder_path):
                    filepath = os.path.join(folder_path, filename)
                    with open(filepath, 'w', encoding='utf-8') as file:
                        json.dump(data, file, ensure_ascii=False, indent=2)
                    print(f"💾 Сохранено: {folder}/{filename}")
            return

        # Для событий группируем по датам.
        events_by_date = {}
        for item in data:
            event_date = datetime.strptime(item['timestamp'], '%Y-%m-%d %H:%M:%S')
            date_str = event_date.strftime("%d-%m-%Y")

            if date_str not in events_by_date:
                events_by_date[date_str] = []
            events_by_date[date_str].append(item)

        # Сохраняем события по датам.
        for date_str, date_events in events_by_date.items():
            folder_path = os.path.join(base_folder, date_str)
            if os.path.exists(folder_path):
                filepath = os.path.join(folder_path, filename)
                with open(filepath, 'w', encoding='utf-8') as file:
                    json.dump(date_events, file, ensure_ascii=False, indent=2)
                print(f"💾 Сохранено: {date_str}/{filename} ({len(date_events)} событий)")

    except Exception as e:
        print(f"❌ Ошибка сохранения {filename}: {e}")


def main():
    # Основная функция генерации данных.
    print("🚀 Запуск генерации синтетических данных за 7 дней...")

    # Настройка окружения.
    output_folder = setup_environment()

    # Генерация данных.
    users = generate_user_profiles(150)
    events = generate_user_behavior(1000, 150)

    # Сохранение данных по дням.
    save_data_to_multiple_days(users, 'user_profiles.json', output_folder)
    save_data_to_multiple_days(events, 'user_behavior_events.json', output_folder)

    # Статистика.
    print("\n📈 Статистика генерации:")
    print(f"   • Пользователей: {len(users)}")
    print(f"   • Событий: {len(events)}")
    print(f"   • Период событий: последние 7 дней")
    print(f"   • Папка с данными: {output_folder}")
    print("\n✅ Генерация данных завершена успешно!")


if __name__ == "__main__":
    main()
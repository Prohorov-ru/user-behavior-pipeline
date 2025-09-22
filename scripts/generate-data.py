# Генерация синтетических данных поведения пользователей и самих пользователей в JSON файл.


# Для генерации данных пользователей, используем генератор данных Faker.
# Предварительно скачав библиотеку в проект (pip install faker).
from faker import Faker
import json
import os
import random
from datetime import datetime, timedelta


# Создаем папку
project_root = os.path.join(os.path.dirname(__file__), '..')
project_root = os.path.abspath(project_root)  # Преобразуем в абсолютный путь

generated_data = os.path.join(project_root, "generated_data")
timestamp = datetime.now().strftime("%d-%m-%Y")
timestamp_folder = os.path.join(generated_data, timestamp)
os.makedirs(timestamp_folder, exist_ok=True)

# Инициализация генератора Faker с русскими(!) данными (ФИО, города, номера).
fake = Faker("ru_RU")


# Генерация user_profiles (user_id, user_name, phone_number, email и city).
def generate_user_profiles():
    users = []

    for i in range(1, 151):
        user = {
            'user_id': f'user_{i}',
            'name': fake.name(),
            'email': fake.email(),
            'city': fake.city(),
            'phone': fake.phone_number()
        }
        users.append(user)
    return users


# Сохраняем профили в отдельную папку timestamp_folder
users = generate_user_profiles()
user_profiles_path = os.path.join(timestamp_folder, 'user_profiles.json')
with open(user_profiles_path, 'w', encoding='utf-8') as file:
    json.dump(users, file, ensure_ascii=False, indent=2)


# Генерация активности пользователей
def generate_user_behavior():
    event_types = ['session_start', 'click', 'page_view']
    user_ids = [f'user_{i}' for i in range(1, 151)]
    devices = ['desktop_windows', 'macos', 'android', 'ios']

    event = {
        'timestamp': (datetime.now() - timedelta(days=random.randint(0, 14))).isoformat(sep=' ', timespec='seconds'),
        'event_type': random.choice(event_types),
        'user_id': random.choice(user_ids),
        'device': random.choice(devices),
    }

    return event


# Генерируем 1000 событий
events = [generate_user_behavior() for _ in range(1000)]

# Сохраняем события в отдельную папку timestamp_folder
raw_events_path = os.path.join(timestamp_folder, 'raw_events.json')
with open(raw_events_path, 'w', encoding='utf-8') as file:
    json.dump(events, file, ensure_ascii=False, indent=2)
    
-- Создаем базу если не существует.
SELECT 'CREATE DATABASE user_behavior'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'user_behavior')\gexec

-- Подключаемся к базе.
\c user_behavior;

-- Создаем таблицы.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    phone VARCHAR(20),
    registration_date DATE
);

CREATE TABLE IF NOT EXISTS user_events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50),
    event_timestamp TIMESTAMP,
    device VARCHAR(50),
    session_id VARCHAR(50),
    page_url TEXT,
    FOREIGN KEY (user_id) REFERENCES user_profiles(user_id)
);

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON user_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON user_events(event_type);
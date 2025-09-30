# User Behavior Pipeline
Аналитика поведения пользователей с Airflow, PostgreSQL, MinIO и Grafana.

## О проекте
Пайплайн для генерации синтетических данных о поведении пользователей, их загрузки в MinIO (S3-совместимое хранилище), обработки и визуализации в Grafana.

## 🚀 Быстрый старт

### 1. Запуск контейнеров
```bash
docker-compose up -d
```
### 2. Доступ к сервисам.
- Airflow: http://localhost:8080 (логин: `airflow`, пароль: `airflow`)
- MinIO: http://localhost:9001 (логин: `minio`, пароль: `minio123`)
- Grafana: http://localhost:3000 (логин: `admin`, пароль: `admin`)

### 3. Запуск пайплайна
1. Откройте Airflow UI
2. Найдите DAG `initial_data_pipeline`
3. Запустите вручную (кнопка "Trigger DAG") - ОДИН РАЗ
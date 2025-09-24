# User Behavior Pipeline
Аналитика поведения пользователей с Airflow, PostgreSQL, MinIO и Grafana.

## О проекте
Данный проект использует синтетические данные, созданные с помощью скрипта. Все названия действий пользователя были предположены исходя из логики и представлений о том, что может быть интересно бизнесу в контексте анализа поведения пользователей на главной странице приложения.

## Цели
* Понять как работают инструменты визуализации и аналитики данных (Grafana)
* Научиться создавать DAGs в Apache Airflow  
* Попробовать поработать с S3 хранилищем (MinIO)
* Попрактиковаться с PostgreSQL и запросами в БД
* Собрать свой первый проект

## 🚀 Быстрый старт

### 1. Клонирование репозитория 
```bush
git clone https://github.com/prohorov-ru/user-behavior-pipeline
cd user-behavior-pipeline
```
### 2. Создание необходимых папок (ВЫПОЛНИТЬ ВРУЧНУЮ)
```bush
mkdir -p dags scripts sql grafana/provisioning grafana/dashboards generated_data
```
### 3. Запуск контейнеров
```bush
docker-compose up -d
```
### 4. Проверка работы
```bush
# Зайдите в контейнер Airflow
docker-compose exec airflow-webserver bash

# Запустите генерацию данных
cd scripts
python generate-data.py

# Выход из контейнера
exit
```
Данные будут сохранены в папку `generated_data/` на хосте.

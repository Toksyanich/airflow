from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from sqlalchemy import create_engine


def postgres_connection():
    try:
        engine = create_engine(
            # убрать в переменные среды
            "postgresql+psycopg2://admin:admin@postgres_data:5432/dwh")
        with engine.connect() as connection:
            result = connection.execute("SELECT version();")
            db_version = result.fetchone()
            print(f"Успешное подключение! Результат запроса: {db_version[0]}")

    except Exception as e:
        print(f"Ошибка подключения: {str(e)}")
        raise


def data_loading_staging():
    None


# Параметры DAG по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
}

# Определение DAG
with DAG(
    'data_process',
    default_args=default_args,
    description='Test PostgreSQL connection DAG using SQLAlchemy engine',
    schedule_interval=None,
    start_date=datetime(2025, 3, 7),
    catchup=False,
) as dag:
    # Определение задачи
    with TaskGroup(group_id='Init_session') as task_group:
        test_connection_task = PythonOperator(
            task_id='Postgres_connection',
            python_callable=postgres_connection,
        )
    task_group

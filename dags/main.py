from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from airflow.exceptions import AirflowException

# Импорт Hook из airflow-clickhouse-plugin
from clickhouse_driver import Client
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def postgres_connection():
    try:
        engine = create_engine(
            # убрать в переменные среды
            "postgresql+psycopg2://admin:admin@postgres_data:5432/mydb"
        )
        with engine.connect() as connection:
            result = connection.execute("SELECT version();")
            db_version = result.fetchone()
            print(f"Успешное подключение Postgres! Версия: {db_version[0]}")
    except Exception as e:
        print(f"Ошибка подключения к Postgres: {e}")
        raise


def data_loading_staging():
    try:
        csv_path = '/opt/airflow/dags/Students.csv'
        df = pd.read_csv(
            csv_path,
            sep=',',
            quotechar='"',
            engine='python',
            on_bad_lines='warn',
            usecols=[
                'Student_ID', 'Age', 'Gender', 'Academic_Level', 'Country',
                'Avg_Daily_Usage_Hours', 'Most_Used_Platform',
                'Affects_Academic_Performance', 'Sleep_Hours_Per_Night',
                'Mental_Health_Score', 'Relationship_Status',
                'Conflicts_Over_Social_Media', 'Addicted_Score'
            ]
        )
        df = df.rename(columns={
            'Student_ID': 'student_id',
            'Age': 'age',
            'Gender': 'gender',
            'Academic_Level': 'academic_level',
            'Country': 'country',
            'Avg_Daily_Usage_Hours': 'avg_daily_usage_hours',
            'Most_Used_Platform': 'most_used_platform',
            'Affects_Academic_Performance': 'affects_academic_performance',
            'Sleep_Hours_Per_Night': 'sleep_hours_per_night',
            'Mental_Health_Score': 'mental_health_score',
            'Relationship_Status': 'relationship_status',
            'Conflicts_Over_Social_Media': 'conflicts_over_social_media',
            'Addicted_Score': 'addicted_score'
        })
        df = df.astype({
            'student_id': 'int',
            'age': 'int',
            'avg_daily_usage_hours': 'float',
            'sleep_hours_per_night': 'float',
            'mental_health_score': 'int',
            'conflicts_over_social_media': 'int',
            'addicted_score': 'int'
        })
        df['affects_academic_performance'] = df['affects_academic_performance'].map({'Yes': True, 'No': False})

        engine = create_engine("postgresql+psycopg2://admin:admin@postgres_data:5432/mydb")
        df.to_sql('students', engine, schema='staging', if_exists='append', index=False)
        print('Данные успешно загружены в staging.students (Postgres)!')
    except Exception as e:
        print(f"Ошибка загрузки данных в Postgres: {e}")
        raise AirflowException(f"Ошибка загрузки данных: {e}")


def postgres_to_clickhouse():
    try:
        # Считываем данные из Postgres
        pg_engine = create_engine("postgresql+psycopg2://admin:admin@postgres_data:5432/mydb")
        df = pd.read_sql_table('students', pg_engine, schema='staging')

        # Вставляем данные в ClickHouse напрямую через clickhouse_driver
        client = Client(
            host='clickhouse_db_1',  # контейнерное имя сервиса из docker-compose
            port=9000,                # нативный TCP-порт внутри сети db_network
            user='admin',
            password='admin',
            database='staging'
        )
        # Формируем список кортежей для вставки
        records = [tuple(r) for r in df.itertuples(index=False, name=None)]
        insert_sql = (
            "INSERT INTO staging.students ("
            "student_id, age, gender, academic_level, country,"
            "avg_daily_usage_hours, most_used_platform,"
            "affects_academic_performance, sleep_hours_per_night,"
            "mental_health_score, relationship_status,"
            "conflicts_over_social_media, addicted_score) VALUES"
        )
        client.execute(
            insert_sql,
            records,
            settings={
                'max_partitions_per_insert_block': 1000
            }
        )
        print('Данные успешно загружены в ClickHouse!')
    except Exception as e:
        print(f"Ошибка загрузки данных в ClickHouse: {e}")
        raise AirflowException(f"Ошибка загрузки данных в ClickHouse: {e}")

# Параметры DAG
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
}

with DAG(
    dag_id='data_process',
    default_args=default_args,
    description='Load student usage data: CSV → Postgres → ClickHouse',
    schedule_interval=None,
    start_date=datetime(2025, 3, 7),
    catchup=False,
) as dag:

    with TaskGroup(group_id='Init_session') as task_group:
        students_connection_task = PythonOperator(
            task_id='Postgres_connection',
            python_callable=postgres_connection,
        )
        load_csv_task = PythonOperator(
            task_id='Load_CSV_to_Postgres',
            python_callable=data_loading_staging,
        )
        students_connection_task >> load_csv_task

    load_to_ch_task = PythonOperator(
        task_id='Postgres_to_ClickHouse',
        python_callable=postgres_to_clickhouse,
    )

    task_group >> load_to_ch_task

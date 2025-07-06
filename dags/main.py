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

        # Define schema mapping once to avoid repetitive work per chunk
        column_rename_map = {
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
        }
        # Explicit dtypes for faster parsing & lower memory
        dtypes = {
            'Student_ID': 'int32',
            'Age': 'int8',
            'Avg_Daily_Usage_Hours': 'float32',
            'Sleep_Hours_Per_Night': 'float32',
            'Mental_Health_Score': 'int16',
            'Conflicts_Over_Social_Media': 'int8',
            'Addicted_Score': 'int16'
        }

        engine = create_engine(
            "postgresql+psycopg2://admin:admin@postgres_data:5432/mydb",
            pool_pre_ping=True
        )

        # Stream the CSV in manageable chunks instead of loading it all at once
        for chunk in pd.read_csv(
            csv_path,
            sep=',',
            quotechar='"',
            engine='python',  # faster C-engine ‑ falls back automatically on bad lines
            on_bad_lines='warn',
            usecols=list(column_rename_map.keys()),
            dtype=dtypes,
            chunksize=50_000  # tune based on available memory / file size
        ):
            chunk = chunk.rename(columns=column_rename_map, copy=False)
            # Convert Yes/No → boolean without allocating a new column twice
            chunk['affects_academic_performance'] = chunk['affects_academic_performance'].eq('Yes')

            # Write in batches – method='multi' groups inserts into single statement
            chunk.to_sql(
                'students',
                engine,
                schema='staging',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10_000,
            )
        print('Данные успешно загружены в staging.students (Postgres)!')
    except Exception as e:
        print(f"Ошибка загрузки данных в Postgres: {e}")
        raise AirflowException(f"Ошибка загрузки данных: {e}")


def postgres_to_clickhouse():
    try:
        pg_engine = create_engine(
            "postgresql+psycopg2://admin:admin@postgres_data:5432/mydb",
            pool_pre_ping=True,
        )

        client = Client(
            host='clickhouse_db_1',
            port=9000,
            user='admin',
            password='admin',
            database='staging',
            compression='lz4'
        )

        # Stream data from Postgres and push to ClickHouse in batches to avoid OOM
        for df_chunk in pd.read_sql_query(
            'SELECT * FROM staging.students',
            pg_engine,
            chunksize=100_000,
        ):
            client.insert_dataframe(
                'INSERT INTO staging.students VALUES',
                df_chunk
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

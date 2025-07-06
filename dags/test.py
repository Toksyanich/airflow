import psycopg2
from psycopg2 import sql
import logging

logger = logging.getLogger(__name__)


def test_postgres_connection():
    conn = None
    try:
        # Явное указание параметров подключения
        conn = psycopg2.connect(
            host="postgres",      # Адрес сервера
            port="5433",           # Порт PostgreSQL
            user="spasskiy_mi",  # Имя пользователя
            password="123",  # Пароль
            dbname="mydb"     # Название базы данных
        )

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1;")
            db_version = cursor.fetchone()
            if db_version:
                logger.info(f"Успешное подключение! Версия PostgreSQL: {db_version[0]}")
            else:
                logger.warning("Не удалось получить версию PostgreSQL")

    except Exception as e:
        logger.exception("Ошибка подключения")
        raise

    finally:
        if conn:
            conn.close()


test_postgres_connection()

# Data Pipeline CSV → PostgreSQL → ClickHouse с Apache Airflow

## Описание
Этот репозиторий содержит полный **ETL-конвейер** для загрузки и анализa данных об использовании социальных сетей студентами.\
Структура слоя **staging** в PostgreSQL повторяется в ClickHouse, что позволяет выполнять быстрый аналитический SQL без избыточных преобразований.

Пайплайн реализован в виде DAG-графа Apache Airflow и развёрнут в Docker-окружении вместе с:
* **PostgreSQL** — хранилище слоя staging
* **ClickHouse** — OLAP-движок для аналитических запросов
* **Redis** — брокер заданий Celery/Triggerer (исп. Airflow)
* **Prometheus + Grafana + cAdvisor** — мониторинг контейнеров и сервисов

## Быстрый старт
1. Предварительно установите **Docker >= 20.10** и **Docker Compose >= 2.0**.  
2. Клонируйте репозиторий и перейдите в каталог проекта:
   ```bash
   git clone <repo-url>
   cd <project-dir>
   ```
3. Запустите стек в фоновом режиме:
   ```bash
   docker compose up -d
   ```
4. Дождитесь, пока сервисы пройдут health-check (≈1-2 мин).  
5. Откройте UI-интерфейсы:
   * Airflow:   <http://localhost:8080>  `airflow/airflow`
   * Grafana:   <http://localhost:3000>  `admin/admin`
   * Prometheus: <http://localhost:9090>
6. В Airflow вручную запустите DAG `data_process` или настройте расписание.

## Архитектура
```text
┌─────────────────┐      CSV      ┌─────────────────┐
│  Students.csv   │ ───────────▶ │ Postgres (staging.students) │
└─────────────────┘               └─────────────────┘
                                             │
                                   SELECT *  │  batch insert
                                             ▼
                                  ┌─────────────────┐
                                  │ ClickHouse (staging.students) │
                                  └─────────────────┘
```

## Компоненты репозитория
| Путь | Назначение |
|------|------------|
| `dags/` | DAG `main.py`, тестовое подключение `test.py`, исходный CSV |
| `scripts/postgres_init/` | DDL скрипты для создания схемы staging в PostgreSQL |
| `scripts/clickhouse_init/` | DDL скрипты для ClickHouse |
| `monitoring/` | Конфигурация Prometheus и Grafana |
| `config/airflow.cfg` | Пользовательская конфигурация Airflow |
| `docker-compose.yaml` | Единый декларативный запуск всего стека |

## Подробности DAG `data_process`
| Task id | Описание |
|---------|----------|
| `Postgres_connection` | Проверка подключения и логирование версии Postgres |
| `Load_CSV_to_Postgres` | Построчная загрузка `Students.csv` (chunksize = 50k) в таблицу `staging.students` Postgres |
| `Postgres_to_ClickHouse` | Пакетная выгрузка из Postgres и вставка в ClickHouse (chunksize = 100k) |

Задание сгруппировано в `TaskGroup Init_session` для наглядности.

## Учётные данные по умолчанию
| Service | Host:Port | User | Password | DB/Scheme |
|---------|-----------|------|----------|-----------|
| PostgreSQL (Airflow metastore) | `postgres:5432` | `airflow` | `airflow` | `airflow` |
| PostgreSQL (staging) | `localhost:5433` | `admin` | `admin` | `mydb` |
| ClickHouse | `localhost:9001` | `admin` | `admin` | `staging` |

Учётные данные можно переопределить через переменные окружения Docker Compose (см. `docker-compose.yaml`).

## Мониторинг
* **cAdvisor** собирает метрики контейнеров и экспортирует их в Prometheus.
* **Prometheus** периодически опрашивает `cadvisor:8080` (scrape_interval = 15s).
* **Grafana** содержит источник данных Prometheus; создайте или импортируйте dashboard после первого запуска.

## Запуск тестов
```bash
docker compose exec airflow-worker python /opt/airflow/dags/test.py
```

## Развитие проекта
- [ ] Реализовать инкрементальную загрузку (CDC) из Postgres в ClickHouse
- [ ] Добавить dbt-модели для слоя mart
- [ ] Автоматизировать обновление дашбордов Grafana

## Лицензия
Проект распространяется под лицензией Apache-2.0.
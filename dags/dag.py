from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path  
import pandas as pd

# Cosmos dbt imports
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

from load_to_sql.create_properties_table import create_properties_tables
from transform.clean_data import run
from load_to_sql.load_raw_data import load_data_to_postgres

# Cấu hình đường dẫn dbt
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_real_estate")
CLEAN_DATA_FILE = "/opt/airflow/data/clean_data.csv"

default_args = {
    "owner": "DucNguyen",
    "start_date": datetime(2025, 12, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

def check_data_not_empty():
    """
    Hàm kiểm tra dữ liệu: Trả về True nếu có dữ liệu, 
    trả về False để ngắt (skip) các task phía sau.
    """
    if not Path(CLEAN_DATA_FILE).exists():
        return False
    df = pd.read_csv(CLEAN_DATA_FILE)
    return not df.empty

with DAG(
    dag_id="etl_real_estate_dag",
    default_args=default_args,
    schedule_interval="0 17 * * *",
    catchup=False,
    max_active_runs=1,
    description="Full ETL DAG với kiểm tra tính toàn vẹn dữ liệu tự động",
) as dag:

    start = EmptyOperator(task_id="start")

    scrape_task = BashOperator(
        task_id="web_scraping",
        bash_command="scrapy crawl real_estate.spiders -O /opt/airflow/data/raw_data.json",
        cwd="/opt/airflow",
    )

    clean_task = PythonOperator(
        task_id="clean_raw_data",
        python_callable=run, 
    )

    check_data_task = ShortCircuitOperator(
        task_id="check_if_data_empty",
        python_callable=check_data_not_empty,
    )

    create_table_in_postgres_task = PythonOperator(
        task_id="create_properties_table_postgresql",
        python_callable=create_properties_tables,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_data_to_postgres,
    )

    dbt_deps = BashOperator(
        task_id="dbt_install_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt deps",
    )

    postgres_dbt = DbtTaskGroup(
        group_id="real_estate_dbt_pipeline",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="real_estate",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="re_connection", 
                profile_args={"schema": "public"},
            ),
        ),
        render_config=RenderConfig(
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    end = EmptyOperator(task_id="end")

    # ---- Workflow Cập Nhật ----
    (
        start 
        >> scrape_task 
        >> clean_task 
        >> check_data_task  # Kiểm tra ngay sau khi clean
        >> create_table_in_postgres_task 
        >> load_task 
        >> dbt_deps 
        >> postgres_dbt 
        >> end
    )
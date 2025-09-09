import os 
from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from utils.enricher import extract_and_enrich
from utils.cleaner import cleaner
from utils.transform_load import transform_and_load

POSTGRES_CONN_ID = "postgresql"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = "capstone_pipeline_dag",
    default_args = default_args,
    schedule = "@daily",
    catchup = False,
    description = "Pipeline of Internship Capstone: Movie Analytics System",
    tags = ["capstone", "ETL Pipeline"]
) as dag:
     
    pg_conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    pg_host = pg_conn.host
    pg_port = pg_conn.port
    pg_dbname = pg_conn.schema
    
    extract_task = PythonOperator(
        task_id = "extract_and_enrich_task",
        python_callable = extract_and_enrich,
    )

    clean_task = PythonOperator(
        task_id = "cleaning_task",
        python_callable = cleaner,
    )
    
    modeling_task= PostgresOperator(
        task_id = "modeling",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="sql/modeling.sql"
    )

    transform_and_load_task = PythonOperator(
        task_id = "transform_and_load",
        python_callable = transform_and_load,
        op_kwargs={
            "pg_url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_dbname}",
            "pg_user": pg_conn.login,
            "pg_pass": pg_conn.password
        }
    )
    
    analysis1_task = PostgresOperator(
        task_id = "analysis1_task",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="sql/analysis1.sql"
    )

    analysis2_task = PostgresOperator(
        task_id = "analysis2_task",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="sql/analysis2.sql"
    )

    analysis3_task = PostgresOperator(
        task_id = "analysis3_task",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="sql/analysis3.sql"
    )

    analysis4_task = PostgresOperator(
        task_id = "analysis4_task",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql="sql/analysis4.sql"
    )



    extract_task >> clean_task >> modeling_task >> transform_and_load_task

    transform_and_load_task>> [analysis1_task, analysis2_task, analysis3_task, analysis4_task]


from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROFILES_DIR = '/opt/airflow/dbt/trans_snowf'
DBT_PROJECT_DIR  = '/opt/airflow/dbt/trans_snowf'

DBT_ENV = {
    'DBT_PROFILES_DIR': DBT_PROFILES_DIR,
    'DBT_PROJECT_DIR':  DBT_PROJECT_DIR,
    'PATH': '/root/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
}

# Shared flags injected into every dbt command
DBT_FLAGS = f'--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}'

default_args = {
    'owner': 'risheek',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ------------------------------------------------------------------
# DAG 1: Bronze layer (raw → typed staging tables)
# ------------------------------------------------------------------
@dag(
    dag_id='banking_01_bronze_layer',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'bronze', 'incremental']
)
def bronze_pipeline():

    debug = BashOperator(
        task_id='debug',
        bash_command=f'dbt debug {DBT_FLAGS}',
        env=DBT_ENV
    )

    compile_ = BashOperator(
        task_id='compile',
        bash_command=f'dbt compile {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_acc = BashOperator(
        task_id='build_bronze_acc',
        bash_command=f'dbt build --select bronze_acc {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_cust = BashOperator(
        task_id='build_bronze_cust',
        bash_command=f'dbt build --select bronze_cust {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_trans = BashOperator(
        task_id='build_bronze_trans',
        bash_command=f'dbt build --select bronze_trans {DBT_FLAGS}',
        env=DBT_ENV
    )

    # debug → compile first, then all three models run in parallel
    compile_ >> [build_acc, build_cust, build_trans]

bronze_pipeline()


# ------------------------------------------------------------------
# DAG 2: Silver layer (bronze → cleaned/joined tables)
# ------------------------------------------------------------------
@dag(
    dag_id='banking_02_silver_layer',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'silver', 'incremental']
)
def silver_pipeline():

    debug = BashOperator(
        task_id='debug',
        bash_command=f'dbt debug {DBT_FLAGS}',
        env=DBT_ENV
    )

    compile_ = BashOperator(
        task_id='compile',
        bash_command=f'dbt compile {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_acc = BashOperator(
        task_id='build_silver_acc',
        bash_command=f'dbt build --select silver_acc {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_cust = BashOperator(
        task_id='build_silver_cust',
        bash_command=f'dbt build --select silver_cust {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_trans = BashOperator(
        task_id='build_silver_trans',
        bash_command=f'dbt build --select silver_trans {DBT_FLAGS}',
        env=DBT_ENV
    )

    compile_ >> [build_acc, build_cust, build_trans]

silver_pipeline()


# ------------------------------------------------------------------
# DAG 3: Gold layer (silver → business-level views/tables)
# ------------------------------------------------------------------
@dag(
    dag_id='banking_03_gold_layer',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'gold', 'view']
)
def gold_pipeline():

    debug = BashOperator(
        task_id='debug',
        bash_command=f'dbt debug {DBT_FLAGS}',
        env=DBT_ENV
    )

    compile_ = BashOperator(
        task_id='compile',
        bash_command=f'dbt compile {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_acc = BashOperator(
        task_id='build_gold_acc',
        bash_command=f'dbt build --select gold_acc {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_cust = BashOperator(
        task_id='build_gold_cust',
        bash_command=f'dbt build --select gold_cust {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_trans = BashOperator(
        task_id='build_gold_trans',
        bash_command=f'dbt build --select gold_trans {DBT_FLAGS}',
        env=DBT_ENV
    )

    compile_ >> [build_acc, build_cust, build_trans]

gold_pipeline()


# ------------------------------------------------------------------
# DAG 4: Aggregation layer (gold → summary analytics tables)
# ------------------------------------------------------------------
@dag(
    dag_id='banking_04_aggregation_layer',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['banking', 'gold', 'aggregations']
)
def aggregation_pipeline():

    build_aggregations = BashOperator(
        task_id='build_aggregations',
        bash_command=f'dbt build --select path:models/aggregations {DBT_FLAGS}',
        env=DBT_ENV
    )

    build_aggregations  # single terminal task — no chain needed

aggregation_pipeline()
from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="transfer_from_s3_to_snowflake_transactions",
    start_date=datetime(2026, 2, 7),
    schedule=None,
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    })
def transfer_data_transactions():

    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id="warehouse_id",
        sql="""
            CREATE TABLE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.RAW_TRANSACTIONS (
                raw_json VARIANT,
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """
    )

    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id="warehouse_id",
        sql="""
            CREATE STAGE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.incoming_transactions
            URL = 's3://banking-database-dump-dev/'
            STORAGE_INTEGRATION = my_s3_integration
            FILE_FORMAT = (TYPE = 'JSON');
        """
    )

    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO BANKING_DB.PUBLIC_RAW.RAW_TRANSACTIONS (raw_json)
            FROM @BANKING_DB.PUBLIC_RAW.incoming_transactions/transactions/
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
    )

    create_bronze_table >> create_stage >> load_json

transfer_data_transactions()


@dag(
    dag_id="transfer_from_s3_to_snowflake_accounts",
    start_date=datetime(2026, 2, 7),
    schedule=None,
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    })
def transfer_data_accounts():

    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id="warehouse_id",
        sql="""
            CREATE TABLE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.RAW_ACCOUNTS (
                raw_json VARIANT,
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """
    )

    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id="warehouse_id",
        sql="""
            CREATE STAGE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.incoming_accounts
            URL = 's3://banking-database-dump-dev/'
            STORAGE_INTEGRATION = my_s3_integration
            FILE_FORMAT = (TYPE = 'JSON');
        """
    )

    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO BANKING_DB.PUBLIC_RAW.RAW_ACCOUNTS (raw_json)
            FROM @BANKING_DB.PUBLIC_RAW.incoming_accounts/accounts/
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
    )

    create_bronze_table >> create_stage >> load_json

transfer_data_accounts()


@dag(
    dag_id="transfer_from_s3_to_snowflake_customers",
    start_date=datetime(2026, 2, 7),
    schedule=None,
    tags=["transfer"],
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    })
def transfer_data_customers():

    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_raw",
        conn_id="warehouse_id",
        sql="""
            CREATE TABLE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.RAW_CUSTOMERS (
                raw_json VARIANT,
                inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
        """
    )

    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id="warehouse_id",
        sql="""
            CREATE STAGE IF NOT EXISTS BANKING_DB.PUBLIC_RAW.incoming_customers
            URL = 's3://banking-database-dump-dev/'
            STORAGE_INTEGRATION = my_s3_integration
            FILE_FORMAT = (TYPE = 'JSON');
        """
    )

    load_json = SQLExecuteQueryOperator(
        task_id="copy_json_to_variant",
        conn_id="warehouse_id",
        sql="""
            COPY INTO BANKING_DB.PUBLIC_RAW.RAW_CUSTOMERS (raw_json)
            FROM @BANKING_DB.PUBLIC_RAW.incoming_customers/customers/
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
        """
    )

    create_bronze_table >> create_stage >> load_json

transfer_data_customers()
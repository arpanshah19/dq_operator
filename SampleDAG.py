from airflow import DAG
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dq_operator import create_dq_taskgroup

def snowflake_executor(sql: str):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    hook.run(sql)

with DAG("sales_dq_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    dq_group = create_dq_taskgroup(
        group_id="sales_dq",
        sql_executor=snowflake_executor
    )



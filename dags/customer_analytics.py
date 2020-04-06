from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import settings

import os
import os.path
from datetime import datetime


home = os.environ["AIRFLOW_HOME"]
sql_path = os.path.join(home, "dags/customer_analytics")

dag = DAG(
    "customer_analytics",
    schedule_interval=None,
    default_args=settings.default_args,
    start_date=datetime(2020, 3, 30, 7),
)


with dag:
    start_dag = LatestOnlyOperator(task_id="start", dag=dag)
    end_dag = DummyOperator(task_id="end", dag=dag)

    jobs = {}
    # Set up the operators
    (_, _, files) = next(os.walk(sql_path), (None, None, []))
    for f in files:
        # Parse script into sql
        sql_stmts = settings.parse_sql(os.path.join(sql_path, f))

        # Add job
        task_name = os.path.splitext(f.lower())[0]
        jobs[task_name] = SnowflakeOperator(
            task_id=task_name,
            snowflake_conn_id="snowflake_default",
            warehouse=Variable.get("warehouse"),
            sql=sql_stmts,
            dag=dag,
        )

    jobs["create_order_detail"] >> jobs["create_meta_order_detail"]
    start_dag >> [jobs["create_calendar"], jobs["create_order_detail"]] >> jobs[
        "create_customer"
    ] >> end_dag


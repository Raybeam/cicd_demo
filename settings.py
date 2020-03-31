from airflow.models import Variable
from datetime import timedelta
import sqlparse

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "schedule_interval": "@daily",
    "max_active_runs": 1,
    "catchup": False,
    "email": Variable.get("email_list", deserialize_json=True),
    "email_on_retry": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def parse_sql(script_path):
    """Returns a list of all SQL statements since the
    SnowflakeOperator can't split the SQL by itself"""

    # Slurp file
    sql = None
    with open(script_path) as x:
        sql = x.read()
    # Split for use in operator
    sql = sqlparse.format(sql, strip_comments=True)
    sql_stmts = sqlparse.split(sql)
    return sql_stmts


from airflow.decorators import dag,task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.operators.empty import EmptyOperator
# adjust for other database types
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pendulum import datetime
import include.sql.alarms as sql_stmts
import os


# CONNECTION DEFINITIONS
SNOWFLAKE_CONNECTION_ID = "snowflake_conn"
SLACK_CONNECTION_ID = "my_slack_conn"


# DBT DEFINITIONS
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt"
## virtual environment for dbt
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"


profile_config = ProfileConfig(
    profile_name="deel-challenge",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONNECTION_ID
    ),
    
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# Slack message
SLACK_MESSAGE = """
    Hello! Here are the organization_id that their balance changes more than 50 percent day over day: {{ ti.xcom_pull(task_ids='sql_query_alarm') }}
"""
SLACK_CHANNEL = "testeo"


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    # defining the directory where SQL templates are stored
    template_searchpath="/usr/local/airflow/include/sql/",
)
def deel_challenge():

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

    sql_query_alarm = SQLExecuteQueryOperator(
        task_id="sql_query_alarm",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=sql_stmts.sql_query_alarm,
        default_args={"retries": 2},
    )

    slack_notifier=SlackNotifier(
                slack_conn_id=SLACK_CONNECTION_ID,
                text=SLACK_MESSAGE,
                channel=SLACK_CHANNEL,
            )
        
    #Since slacknotifier is not an operator we need to create an operator 
    task_empty = EmptyOperator(
        task_id="send_alarm", 
        on_success_callback=[slack_notifier])




    transform_data >> sql_query_alarm >> task_empty

deel_challenge()

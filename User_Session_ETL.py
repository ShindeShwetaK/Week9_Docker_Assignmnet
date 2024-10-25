#Step 1 Import all required modules
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
import snowflake.connector


# Default arguments for the DAG with owner info and retries info
default_args = {
    'owner': 'Shweta',
    'email_on_failure': False,
    'start_date': days_ago(1),
    'retries': 1,
}


create_blob_stage = """
CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

create_load_tables_user = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);

COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;
"""

create_load_tables_session= """
CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);

COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

# Define the DAG with the Snowflake tasks
with DAG(
    dag_id='WAU_STEP1_Dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    # Task to create the stage to access S3
    create_stage_task = SnowflakeOperator(
        task_id='create_stage',
        snowflake_conn_id='snowflake_conn',
        sql=create_blob_stage,
    )
 
 
    # Task to create the user_session_channel table and load data from S3
    create_load_tables_user_task = SnowflakeOperator(
        task_id='create_load_tables_user',
        snowflake_conn_id='snowflake_conn',
        sql=create_load_tables_user,
    )

 

    # Task to create the user_session_channel table and load data from S3
    create_load_tables_session_task = SnowflakeOperator(
        task_id='create_load_tables_session',
        snowflake_conn_id='snowflake_conn',
        sql=create_load_tables_session,
    )

    # Set task dependencies
    create_stage_task >> create_load_tables_user_task 
    create_stage_task >> create_load_tables_session_task
    
    


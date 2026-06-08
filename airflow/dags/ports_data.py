from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 5, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'aws_conn_id': 'None'
}
with DAG(
    dag_id='ports_monthly_pipeline',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
    catchup=False,
    tags=['marisight', 'monthly']
) as dag:

    trigger_ports = LambdaInvokeFunctionOperator(
        task_id='trigger_ports_lambda',
        function_name='Marisight_Ports_API',
        payload=json.dumps({"job_type": "monthly"}),
        invocation_type='Event',
        log_type='Tail'
    )

    verify_ports_s3 = S3KeySensor(
        task_id='verify_ports_in_s3',
        bucket_name='marisight-staging-layer-121913093195-us-east-1-an',
        bucket_key="ports/api_{{ logical_date.strftime('%Y-%m') }}.csv",
        poke_interval=60,
        timeout=1200
    )

    copy_ports_snowflake = SnowflakeOperator(
        task_id='copy_ports_snowflake',
        snowflake_conn_id='snowflake_1',
        sql="""
            COPY INTO Project_DB.DBO.ports
            FROM @Project_DB.DBO.s3_stage
            FILES = ('api_{{ logical_date.strftime('%Y-%m') }}.csv')
            FILE_FORMAT = (
                TYPE = 'CSV'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = 'CONTINUE';
        """
    )
    dbt_transform_ports = BashOperator(
    task_id='dbt_transform_ports',
    bash_command='cd /opt/airflow/dags/marisight_dbt && dbt build --select silver_ports+'
    )

    trigger_ports >> verify_ports_s3 >> copy_ports_snowflake >> dbt_transform_ports
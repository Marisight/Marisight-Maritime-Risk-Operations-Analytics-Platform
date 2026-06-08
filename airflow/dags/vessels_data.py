from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 5, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'aws_conn_id': None
}

with DAG(
    dag_id='vessels_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 17 * * *',
    catchup=False,
    tags=['marisight', 'daily']
) as dag:

    verify_vessels_s3 = S3KeySensor(
        task_id='verify_vessels_in_s3',
        bucket_name='marisight-staging-layer-121913093195-us-east-1-an',
        bucket_key=(
            "vessels/year={{ data_interval_end.in_timezone('Africa/Cairo').strftime('%Y') }}"
            "/month={{ data_interval_end.in_timezone('Africa/Cairo').strftime('%m') }}"
            "/vessels_{{ data_interval_end.in_timezone('Africa/Cairo').strftime('%Y-%m-%d') }}.csv"
        ),
        poke_interval=60,
        timeout=1800
    )

    copy_into_snowflake = SnowflakeOperator(
            task_id='copy_into_snowflake',
            snowflake_conn_id='snowflake_1',
            sql="""
                DELETE FROM PROJECT_DB.DBO.vessel 
                WHERE report_date LIKE '{{ data_interval_end.in_timezone("Africa/Cairo").strftime("%Y-%m-%d") }}%';

                COPY INTO PROJECT_DB.DBO.vessel
                FROM (
                    SELECT 
                        t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18,
                        CURRENT_TIMESTAMP()
                    FROM @PROJECT_DB.DBO.vessels_s3_stage/year={{ data_interval_end.in_timezone("Africa/Cairo").strftime("%Y") }}/month={{ data_interval_end.in_timezone("Africa/Cairo").strftime("%m") }}/vessels_{{ data_interval_end.in_timezone("Africa/Cairo").strftime("%Y-%m-%d") }}.csv t
                )
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                )
                FORCE = TRUE;
            """
        )
    dbt_run_vessels = BashOperator(
        task_id='dbt_run_vessels',
        bash_command='cd /opt/airflow/dags/marisight_dbt && dbt build --select silver_vessels+'
    )
    # بعد تعريف مهمة الـ dbt الحالية، أضيفي مهمة التوصيات:
    gold_port_recommendations = BashOperator(
        task_id="gold_port_recommendations",
        bash_command="cd /opt/airflow/dags && python port_recommendations_engine.py"
    )

    verify_vessels_s3 >> copy_into_snowflake >> dbt_run_vessels >> gold_port_recommendations

    
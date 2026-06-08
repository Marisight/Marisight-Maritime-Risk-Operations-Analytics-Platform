from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='seismic_transform_pipeline',
    default_args=default_args,
    # هنا التعديل عشان يشتغل مرة واحدة كل يوم (مثلاً الساعة 2 صباحاً)
    schedule_interval='@daily', 
    catchup=False,
    tags=['marisight', 'seismic']
) as dag:

    # تنفيذ تحويلات الـ dbt لبيانات الـ seismic الموضحة في image_3f5f1e.png
    dbt_transform_seismic = BashOperator(
    task_id='dbt_transform_seismic',
    # المسار ده ثابت دلوقتي مهما نقلتي البروجيكت على أي جهاز
    bash_command='cd /opt/airflow/dags/marisight_dbt && dbt build --select silver_seismic_events+'
    )
    

    dbt_transform_seismic
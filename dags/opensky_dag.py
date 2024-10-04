from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from datetime import datetime, timedelta
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from opensky_producer import kafka_producer
from opensky_consumer import s3_consumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'opensky_data_pipeline',
    default_args=default_args,
    description='A DAG to run the OpenSky data pipeline',
    schedule_interval=timedelta(minutes=5),
)

produce_task = PythonOperator(
    task_id='produce_opensky_data',
    python_callable=kafka_producer,
    dag=dag,
)

consume_task = PythonOperator(
    task_id='consume_to_s3',
    python_callable=s3_consumer,
    dag=dag,
)

# load_to_snowflake_task = S3ToSnowflakeOperator(
#     task_id='load_to_snowflake',
#     s3_keys=['raw_data/{{ execution_date.strftime("%Y/%m/%d/%H") }}/*.parquet'],
#     table='FLIGHT_DATA',
#     schema='YOUR_SCHEMA',
#     stage='FLIGHT_DATA_STAGE',
#     file_format='(TYPE = PARQUET)',
#     snowflake_conn_id='snowflake_default',
#     dag=dag,
# )

# aggregate_data_task = SnowflakeOperator(
#     task_id='aggregate_data',
#     sql="""
#     INSERT INTO flight_data_daily_summary
#     SELECT DATE_TRUNC('DAY', timestamp) as date,
#            origin_country,
#            COUNT(*) as flight_count,
#            AVG(velocity) as avg_velocity
#     FROM flight_data
#     WHERE timestamp < DATEADD(day, -30, CURRENT_DATE())
#     GROUP BY 1, 2
#     """,
#     snowflake_conn_id='snowflake_default',
#     dag=dag,
# )

# cleanup_task = SnowflakeOperator(
#     task_id='cleanup_old_data',
#     sql="""
#     DELETE FROM flight_data
#     WHERE timestamp < DATEADD(day, -30, CURRENT_DATE())
#     """,
#     snowflake_conn_id='snowflake_default',
#     dag=dag,
# )

produce_task >> consume_task 
# >> load_to_snowflake_task >> aggregate_data_task >> cleanup_task
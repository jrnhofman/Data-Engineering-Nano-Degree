from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTablesOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'jehofman',
    'start_date': datetime(2020, 9, 16),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_interval': 5*60
}

dag = DAG('etl_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables',
    redshift_conn_id="redshift",
    dag=dag
)

stage_demographics_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographics',
    redshift_conn_id='redshift',
    destination_table='staging_demographics',
    s3_data='s3://jehofman-udacity-dend-capstone-project/demographics',
    aws_credentials_id="aws_credentials",
    s3_jsonpath='auto',
    dag=dag
)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    redshift_conn_id='redshift',
    destination_table='staging_immigration',
    s3_data='s3://jehofman-udacity-dend-capstone-project/immigration',
    aws_credentials_id="aws_credentials",
    s3_jsonpath='auto',
    dag=dag
)

load_immigrant_table = LoadDimensionOperator(
    task_id='Load_immigrant_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.immigrant_table_insert,
    destination_table="immigrant",
    dag=dag
)
load_immigrant_stats_table = LoadFactOperator(
    task_id='Load_immigrant_stats_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.immigrant_stats_table_insert,
    destination_table="immigrant_stats",
    dag=dag
)
load_city_population_table = LoadFactOperator(
    task_id='Load_city_population_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.city_population_table_insert,
    destination_table="city_population",
    dag=dag
)
load_city_demographics_table = LoadFactOperator(
    task_id='Load_city_demographics_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.city_demographics_table_insert,
    destination_table="city_demographics",
    dag=dag
)
load_arrival_info_table = LoadFactOperator(
    task_id='Load_arrival_info_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.arrival_info_table_insert,
    destination_table="arrival_info",
    dag=dag
)
load_arrival_date_table = LoadFactOperator(
    task_id='Load_arrival_date_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.arrival_date_table_insert,
    destination_table="arrival_date",
    dag=dag
)


run_quality_checks = DataQualityOperator(
   task_id='Run_data_quality_checks',
   redshift_conn_id="redshift",
   tables=['immigration','immigration_stats','city_demographics'
           ,'city_population','arrival_info', 'arrival_date'],
   dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks
#
start_operator >> create_tables >> [stage_demographics_to_redshift, stage_immigration_to_redshift] >> load_immigrant_table
load_immigrant_table >> [load_immigrant_stats_table, load_city_demographics_table, load_city_population_table, load_arrival_date_table, load_arrival_info_table] >> run_quality_checks
run_quality_checks >> end_operator

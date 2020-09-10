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
    'start_date': datetime(2020, 8, 26),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_interval': 5*60
}

dag = DAG('etl_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables',
    redshift_conn_id="redshift",
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    destination_table='staging_events',
    s3_data='s3://udacity-dend/log_data',
    aws_credentials_id="aws_credentials",
    s3_jsonpath='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    destination_table='staging_events',
    s3_data='s3://udacity-dend/song_data',
    aws_credentials_id="aws_credentials",
    s3_jsonpath='auto',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    destination_table="songplays",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    destination_table="users",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    destination_table="songs",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    destination_table="artists",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    destination_table="time",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=['users','time','songs','artists','songplays'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#
start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_song_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

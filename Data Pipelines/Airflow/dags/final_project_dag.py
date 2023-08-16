from datetime import datetime, timedelta
import os
import logging
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries as sql


from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import logging
import json


default_args = {
    'owner': 'Sparkify',
    'Depends_on_past': True,
    'start_date': datetime(2023, 8, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup' : False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval = '@daily'
)
def joel_project():
    begin_execution_task = DummyOperator(task_id = 'Begin_execution', dag = dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id = 'Stage_events',        
        redshift_conn_id = 'redshift',
        aws_credentials = 'aws_credentials',
        s3_bucket = Variable.get('s3_bucket'),
        s3_prefix = Variable.get('s3_prefix_log'),
        table = 'staging_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = 'redshift',
        aws_credentials = 'aws_credentials',
        s3_bucket = Variable.get('s3_bucket'),
        s3_prefix = Variable.get('s3_prefix_song'),
        table = 'staging_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag = dag,
        # Add new rows to table without affecting existing data
        append_mode = True,
        redshift_conn_id = 'redshift',
        table = 'songplay',
        insert_query = sql.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag = dag,
        # Add new rows to table without affecting existing data
        append_mode = True,
        redshift_conn_id = 'redshift',
        table = 'users',
        insert_query = sql.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag = dag,
        # Add new rows to table without affecting existing data
        append_mode = True,
        redshift_conn_id = 'redshift',
        table = 'song',
        insert_query = sql.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag = dag,
        # Add new rows to table without affecting existing data
        append_mode = True,
        redshift_conn_id = 'redshift',
        table = 'artist',
        insert_query = sql.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag = dag,
        # Add new rows to table without affecting existing data
        append_mode = True,
        redshift_conn_id = 'redshift',
        table = 'time',
        insert_query = sql.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id = "redshift",
        tables = ["songplays", "songs", "artists",  "time", "users"]
    )

    end_execution_task = DummyOperator(task_id='End_execution', dag = dag)

    # Task ordering for the DAG tasks 
    begin_execution_task >> stage_events_to_redshift
    begin_execution_task >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_execution_task

s3_to_redshift_dag = joel_project()


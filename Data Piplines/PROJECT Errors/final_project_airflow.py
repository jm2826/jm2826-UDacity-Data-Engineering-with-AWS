from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
# import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from final_project_operators.stage_redshift import StageToRedshiftOperator
#from helpers import SqlQueries
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.secrets.metastore import MetastoreBackend

metastoreBackend = MetastoreBackend()
default_args = {
    'owner': 'Joel Magee',
    'start_date': pendulum.now(),
    'Depends_on_past': True,
    #'start_date': datetime(2023, 8, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    #'catchup' : False
}

'''dag = DAG( dag_id ='joel_final_project',
           default_args=default_args,
           description='Load and transform data in Redshift with Airflow',
           schedule_interval = '@hourly
        )'''
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval = '@hourly'
)
def joel_final_project():
    
    
    start_operator = DummyOperator(task_id='Begin_execution')
    
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag = dag,
        metastoreBackend = MetastoreBackend(),
        #conn_id = 'redshift',
        aws_credentials = metastoreBackend.get_connection('aws_credentials'),
        #aws_credentials = S3Hook(aws_conn_id='aws_credentials'),
        s3_bucket = 'joel-airflow', #Variable.get('s3_bucket'),
        s3_prefix = 'log-data', #Variable.get('s3_prefix_log'),
        #table = 'staging_events'
    )

    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag = dag,
        metastoreBackend = MetastoreBackend(),
        #conn_id = 'redshift',
        aws_credentials = metastoreBackend.get_connection('aws_credentials'),
        #aws_credentials = S3Hook(aws_conn_id='aws_credentials'),
        s3_bucket = 'joel-airflow', #Variable.get('s3_bucket'),
        s3_prefix = 'song-data', #Variable.get('s3_prefix_song'),
        #table = 'staging_songs'
    )

final_project_dag = joel_final_project()
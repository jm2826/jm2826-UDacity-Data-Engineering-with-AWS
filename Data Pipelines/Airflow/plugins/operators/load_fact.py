from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common.final_project_sql_statements import SqlQueries as sql
from airflow.secrets.metastore import MetastoreBackend
import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 append_mode,
                 redshift_conn_id,
                 table,
                 insert_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.append_mode = append_mode
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_query = insert_query

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
 
        #Connect to Redshift
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
                num_songs int,
                artist_id varchar,
                artist_latitude float,
                artist_longitude float,
                artist_location varchar,
                artist_name varchar,
                song_id varchar,
                title varchar,
                duration float,
                year int);
        """)

        logging.info(f"{self.table} Created")

        redshift.run(self.insert_query)
        
        logging.info(f"{self.table} New records inserted")

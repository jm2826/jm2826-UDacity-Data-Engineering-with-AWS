from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.tables:
           query = f"""SELECT COUNT(*) FROM {table}"""
           result = redshift.get_records(sql=query)
           table_count = result[0][0] 
           # Log the result
           logging.info(f"Table count: {table_count}")
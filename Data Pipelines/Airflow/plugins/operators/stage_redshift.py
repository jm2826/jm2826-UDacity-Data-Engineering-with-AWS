from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common.final_project_sql_statements import SqlQueries as sql
from airflow.secrets.metastore import MetastoreBackend
import logging


metastoreBackend = MetastoreBackend()

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                aws_credentials,
                s3_bucket,
                s3_prefix,
                table,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table

    def execute(self, context):

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Copy from S3 to Redshift Severless "TABLE Created from Copying JSON Files"
        # json_format = 's3://{self.s3_bucket}/log_json_path.json'

        redshift.run(f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession int,
            lastName varchar,
            length float,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration float,
            sessionid int,
            song varchar,
            status int,
            ts bigint,
            userAgent varchar,
            userid int);
        """)
        logging.info(f"{self.table} Created")

        redshift.run(f"""COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{s3_aws_credentials.login}'
                            SECRET_ACCESS_KEY '{s3_aws_credentials.password}';""") # JSON '{json_format}';""")

        logging.info(f"{self.table} Copied in Redhshift Serverless DEV Database")



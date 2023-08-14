from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.models import Variable
#from airflow.hooks.S3_hook import S3Hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 #conn_id,
                 aws_credentials,
                 s3_bucket,
                 s3_prefix,
                # redshift,
                # table,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        #self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        #self.redshift = redshift
        #self.table = table

    def execute(self, context):
        self.logging.info('StageToRedshiftOperator not implemented yet')
        
        
        
        

    '''  # Connect to S3
        s3_hook = self.aws_credentials
        s3_path = f's3://{self.s3_bucket}/{self.s3_prefix}'
        self.log.info(f's3://{self.s3_bucket}/{self.s3_prefix}')'''




'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id,
                 table,
                 schema,
                 copy_options=None,
                 *args,* *kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args,* *kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.schema = schema
        self.copy_options = copy_options or []

    def execute(self, context):
        self.log.info('Executing S3ToRedshiftOperator')
        
        # Connect to S3
        s3_hook = S3Hook(aws_conn_id='your-aws-connection')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        
        # Connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Generate the COPY command
        copy_command = f"""
            COPY {self.schema}.{self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{{}}'
            SECRET_ACCESS_KEY '{{}}'
            {' '.join(self.copy_options)}
        """
        
        # Execute the COPY command
        redshift_hook.run(copy_command)
        
        self.log.info('S3ToRedshiftOperator execution complete')
In the above code, the S3ToRedshiftOperator class extends the BaseOperator class and overrides the execute method. The operator takes the following parameters:

s3_bucket: The name of the S3 bucket where the data is located.
s3_key: The key or path of the file(s) in the S3 bucket.
redshift_conn_id: The connection ID for the Redshift cluster.
table: The table name in Redshift where the data will be copied.
schema: The schema name in Redshift where the data will be copied.
copy_options: Additional options for the COPY command (e.g., file format, delimiter, etc.).
Inside the execute method, the operator connects to S3 using the S3Hook and generates the COPY command to copy the data from S3 to Redshift. It then connects to Redshift using the PostgresHook and executes the COPY command.

You can use this custom operator in your Airflow DAG by instantiating it and passing the required parameters. Make sure to replace 'your-aws-connection' with the appropriate AWS connection ID.'''

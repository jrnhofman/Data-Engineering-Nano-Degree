from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 s3_data="",
                 aws_credentials_id="",
                 s3_jsonpath='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.s3_data = s3_data
        self.aws_credentials_id = aws_credentials_id
        self.s3_jsonpath = s3_jsonpath

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        staging_copy = ("""
            COPY {} FROM '{}' 
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}' REGION 'us-east-1';
            """).format(self.destination_table, self.s3_data, credentials.access_key,
            credentials.secret_key, self.s3_jsonpath)
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(staging_copy)






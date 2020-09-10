from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names = [],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)      
        for table in self.table_names:
            records = redshift.get_records(f"SELECT count(*) FROM {table};")[0]
            self.log.info(f'DataQualityOperator checking row count in table {table}')
            if records[0] == 0:
                raise ValueError(f"Data quality check failed. {table} retuned no results")
        
        null_table_checks = ['users', 'songs']
        null_column_checks = ['userid', 'song_id']
                              
        for t,c in zip(null_table_checks, null_column_checks):
            records = redshift.get_records(f"SELECT count(CASE WHEN {c} IS NULL THEN 1 END) FROM {t};")[0]
            self.log.info(f'DataQualityOperator checking nulls in table {t}, column{c}')
            if records[0] > 0:
                raise ValueError(f"Data quality check failed. {t} contains NULLS in column {c}")
   
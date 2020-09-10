from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 destination_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.destination_table = destination_table

    def execute(self, context):
        insert_query = f"""
            DROP TABLE IF EXISTS {self.destination_table};
            CREATE TABLE {self.destination_table} 
            AS
            {self.sql_query}
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(insert_query)


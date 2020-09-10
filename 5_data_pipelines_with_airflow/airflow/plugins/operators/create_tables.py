from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        create_tables_staging_events = f"""
            CREATE TABLE IF NOT EXISTS staging_events(
                artist varchar(max)
                , auth varchar(max)
                , firstName varchar(max)
                , gender varchar(max)
                , itemInSession int  
                , lastName varchar(max)
                , length real
                , level varchar(max)
                , location varchar(max)
                , method varchar(max)
                , page varchar(max)
                , registration bigint-- needs to be bigint for range restrictions
                , sessionId varchar(max)
                , song varchar(max)
                , status varchar(max)
                , ts bigint -- needs to be bigint for range restrictions
                , userAgent varchar(max)
                , userId varchar(max)
            );
        """

        create_tables_staging_songs = f"""
            CREATE TABLE IF NOT EXISTS staging_songs(
                num_songs integer
                , artist_id varchar(max)
                , artist_latitude varchar(max)
                , artist_longitude varchar(max)
                , artist_location varchar(max) 
                , artist_name varchar(max)
                , song_id varchar(max)
                , title varchar(max)
                , duration real
                , year int
            );
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(create_tables_staging_songs)
        redshift.run(create_tables_staging_events)

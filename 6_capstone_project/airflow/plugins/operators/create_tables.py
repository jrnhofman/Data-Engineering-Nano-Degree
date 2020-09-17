from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delete_first=True,
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_first = delete_first

    def execute(self, context):
            
        create_table_immigrant = """
            CREATE TABLE IF NOT EXISTS immigrant(
                immigrant_id numeric(18,0) NOT NULL,
                port varchar(256),
                state varchar(256),
                arrival_date varchar(256),
                CONSTRAINT immigrant_pkey PRIMARY KEY (immigrant_id)
        );"""

        create_table_immigrant_stats = """
            CREATE TABLE IF NOT EXISTS immigrant_stats (
                immigrant_id numeric(18,0) NOT NULL,
                immigration_id numeric(18,0),
                gender varchar(256),
                age numeric(18,0),
                origin_country varchar(256),
                visa_type varchar(256),
                visitor_type varchar(256),
                CONSTRAINT immigrant_stats_pkey PRIMARY KEY (immigrant_id)
            );"""

        create_table_city_population = """
            CREATE TABLE IF NOT EXISTS city_population (
                city varchar(256) NOT NULL,
                port varchar(256),
                state varchar(256),
                male_population numeric(18,0),
                female_population numeric(18,0),
                total_population numeric(18,0),
                foreign_born numeric(18,0),
                CONSTRAINT city_pkey PRIMARY KEY (city)
            );;"""

        create_table_city_demographics = """
            CREATE TABLE IF NOT EXISTS city_demographics (
                city varchar(256) NOT NULL,
                port varchar(256),
                state varchar(256),
                race varchar(256),
                n_persons numeric(18,0),
                CONSTRAINT city_demographics_pkey PRIMARY KEY (city)
            );"""

        create_table_arrival_info = """
            CREATE TABLE IF NOT EXISTS arrival_info (
                immigrant_id numeric(18,0) NOT NULL,
                port varchar(256),
                port_type varchar(256),
                airline_code varchar(256),
                airline_flight_number varchar(256),
                CONSTRAINT arrival_info_pkey PRIMARY KEY (immigrant_id)
            );"""

        create_table_arrival_date = """
            CREATE TABLE IF NOT EXISTS arrival_date (
                arrival_date varchar(256) NOT NULL,
                immigration_year numeric(18, 0),
                immigration_month numeric(18,0),
                CONSTRAINT arrival_date_pkey PRIMARY KEY (arrival_date)
            );"""
        
        create_staging_immigration = """
            CREATE TABLE IF NOT EXISTS public.staging_immigration (
                immigrant_id numeric(18,0) NOT NULL,
                immigration_id numeric(18,0),
                immigration_year numeric(18,0),
                immigration_month numeric(18,0),
                origin_country varchar(256),
                arrival_port varchar(256),
                arrival_date varchar(256),
                port_type varchar(256),
                state_of_residence varchar(256),
                immigrant_age numeric(18,0),
                visitor_type varchar(256),
                airline_code varchar(256),
                airline_flight_number varchar(256),
                visa_type varchar(256),
                gender varchar(256)
        );"""
        
        create_staging_demographics = """
            CREATE TABLE IF NOT EXISTS public.staging_demographics (
                city varchar(256),
                state varchar(256),
                male_population numeric(18,0),
                female_population numeric(18,0),
                total_population numeric(18,0),
                foreign_born numeric(18,0),
                race varchar(256),
                n_persons numeric(18,0),
                port varchar(256)
            );"""
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_first:
            redshift.run("drop table if exists immigrant")
            redshift.run("drop table if exists immigrant_stats")
            redshift.run("drop table if exists city_population")
            redshift.run("drop table if exists city_demographics")
            redshift.run("drop table if exists arrival_info")
            redshift.run("drop table if exists arrival_date")
            
            redshift.run("drop table if exists staging_immigration")
            redshift.run("drop table if exists staging_demographics")

        redshift.run(create_table_immigrant)
        redshift.run(create_table_immigrant_stats)
        redshift.run(create_table_city_population)
        redshift.run(create_table_city_demographics)
        redshift.run(create_table_arrival_info)
        redshift.run(create_table_arrival_date)
        redshift.run(create_staging_demographics)
        redshift.run(create_staging_immigration)

import configparser
import os
import datetime

from pyspark.sql import types as t
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'song_data.parquet'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').drop_duplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artist_data.parquet'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    df.registerTempTable('events')
    users_table = spark.sql("""
        SELECT
            userId as user_id, firstName as first_name, lastName as last_name, gender
            -- gets paid if user is both 'paid' and 'free'
            , MAX(level) as level
        FROM events
        WHERE userId IS NOT NULL
        GROUP BY 1,2,3,4
    """).drop_duplicates()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'user_data.parquet'))

    # create timestamp column from original timestamp column
    def extract_datetime(x):
        return datetime.datetime.fromtimestamp(x/1e3).strftime('%Y-%m-%d %H:%M:%S')

    get_timestamp = F.udf(extract_datetime)
    df = df.withColumn('datetime', get_timestamp('ts'))

    # extract columns to create time table
    df.registerTempTable('events')
    time_table = spark.sql("""
        SELECT
            ts
            , extract(hour from ts) as hour
            , extract(day from ts) as day
            , extract(week from ts) as week
            , extract(month from ts) as month
            , extract(year from ts) as year
            , extract(dayofweek from ts) as weekday
        FROM
        (
            SELECT
                datetime as ts
            FROM events
            WHERE userId IS NOT NULL
        )
    """).drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(os.path.join(output_data, 'time_table.parquet'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'song_data.parquet'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artist_data.parquet'))
    song_df.registerTempTable('songs')
    artist_df.registerTempTable('artists')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT
            se.datetime as start_time
            , se.userId as user_id
            -- gets paid if user is both 'paid' and 'free'
            , MAX(se.level) as level
            , ss.song_id
            , ss.artist_id
            , se.sessionId as session_id
            , se.location
            , se.userAgent as user_agent
        FROM events se
        JOIN artists a
            ON se.artist = a.artist_name
        JOIN songs ss
            ON se.song = ss.title
            AND a.artist_id = ss.artist_id
            AND se.length = ss.duration
        WHERE se.userId IS NOT NULL
        GROUP BY 1,2,4,5,6,7,8
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(os.path.join(output_data, 'songplays_table.parquet'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jehofman-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()


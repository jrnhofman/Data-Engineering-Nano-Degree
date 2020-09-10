import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

drop_base_cmd = "DROP TABLE IF EXISTS "

staging_events_table_drop = drop_base_cmd + "staging_events"
staging_songs_table_drop = drop_base_cmd + "staging_songs"

songplay_table_drop = drop_base_cmd + "songplays"
user_table_drop = drop_base_cmd + "users"
song_table_drop = drop_base_cmd + "songs"
artist_table_drop = drop_base_cmd + "artists"
time_table_drop = drop_base_cmd + "time"

# CREATE TABLES

staging_events_table_create= ("""
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
""")

staging_songs_table_create = ("""
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
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id bigint IDENTITY NOT NULL 
        , start_time timestamp NOT NULL
        , user_id varchar(max) distkey
        , level varchar(max) NOT NULL 
        , song_id varchar(max) sortkey
        , artist_id varchar(max)
        , session_id varchar(max)
        , location varchar(max) NOT NULL
        , user_agent varchar(max) 
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar(max) NOT NULL distkey sortkey
        , first_name varchar(max) NOT NULL
        , last_name varchar(max) NOT NULL
        , gender varchar(max) NOT NULL
        , level varchar(max) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar(max) NOT NULL sortkey
        , title varchar(max) NOT NULL
        , artist_id varchar(max) NOT NULL
        , year integer NOT NULL
        , duration real NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar(max) NOT NULL sortkey
        , name varchar(max) NOT NULL
        , location varchar(max)
        , latitude varchar(max)
        , longitude varchar(max)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp NOT NULL sortkey
        , hour integer NOT NULL
        , day integer NOT NULL
        , week integer NOT NULL
        , month integer NOT NULL
        , year integer NOT NULL
        , weekday integer NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}'
    json {} region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'][1:-1], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {} 
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'][1:-1])

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time 
        , user_id 
        , level 
        , song_id 
        , artist_id 
        , session_id 
        , location 
        , user_agent 
        )
    (
        -- remove duplicates
        SELECT
            distinct *
        FROM
        (
            SELECT
                -- default way to go from epoch to timestamp
                -- as redshift doesn't support it with a function
                timestamp 'epoch' + se.ts * interval '1 second' AS start_time
                , se.userId as user_id
                -- gets paid if user is both 'paid' and 'free'
                , MAX(se.level) as level
                , ss.song_id
                , ss.artist_id
                , se.sessionId as session_id
                , se.location
                , se.userAgent as user_agent
            FROM staging_events se
            JOIN staging_songs ss
                ON se.song = ss.title
                AND se.artist = ss.artist_name
                AND se.length = ss.duration
            WHERE se.page = 'NextSong'
            AND se.userId IS NOT NULL
            GROUP BY 1,2,4,5,6,7,8
        )
    )
""")

user_table_insert = ("""
    INSERT INTO users
    (
        -- remove duplicates
        SELECT
            distinct *
        FROM
        (
            SELECT
                userId as user_id, firstName as first_name, lastName as last_name, gender 
                -- gets paid if user is both 'paid' and 'free'
                , MAX(level) as level
            FROM staging_events
            WHERE page = 'NextSong'
            AND userId IS NOT NULL
            GROUP BY 1,2,3,4
        )
    )
""")

song_table_insert = ("""
    INSERT INTO songs
    (
        -- remove duplicates
        SELECT
            distinct *
        FROM
        (
            SELECT
                song_id, title, artist_id, year, duration
            FROM staging_songs
            WHERE song_id IS NOT NULL
        )
    )
""")

artist_table_insert = ("""
    INSERT INTO artists 
    (
        -- remove duplicates
        SELECT
            distinct *
        FROM
        (
            SELECT
                artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
            FROM staging_songs
            WHERE artist_id IS NOT NULL
        )
    )
""")

time_table_insert = ("""
    INSERT INTO time
    (
        -- remove duplicates
        SELECT
            distinct *
        FROM
        (        
            SELECT
                ts
                , extract(hour from ts) as hour
                , extract(day from ts) as day
                , extract(week from ts) as week
                , extract(month from ts) as month
                , extract(year from ts) as year
                , extract(weekday from ts) as weekday
            FROM
            (
                SELECT
                    timestamp 'epoch' + ts * interval '1 second' AS ts
                FROM staging_events
                WHERE page = 'NextSong'
                AND userId IS NOT NULL
            )
        )
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# DROP TABLES

drop_base_cmd = "DROP TABLE IF EXISTS "

songplay_table_drop = drop_base_cmd + "songplays"
user_table_drop = drop_base_cmd + "users"
song_table_drop = drop_base_cmd + "songs"
artist_table_drop = drop_base_cmd + "artists"
time_table_drop = drop_base_cmd + "time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id varchar PRIMARY KEY
        , start_time timestamp NOT NULL
        , user_id varchar NOT NULL
        , level varchar NOT NULL
        , song_id varchar
        , artist_id varchar
        , session_id varchar NOT NULL
        , location varchar NOT NULL
        , user_agent varchar NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar PRIMARY KEY
        , first_name varchar NOT NULL
        , last_name varchar NOT NULL
        , gender varchar NOT NULL
        , level varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY
        , title varchar NOT NULL
        , artist_id varchar NOT NULL
        , year int NOT NULL
        , duration float NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY
        , name varchar NOT NULL
        , location varchar
        , latitude float
        , longitude float
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp PRIMARY KEY
        , hour int NOT NULL
        , day int NOT NULL
        , week int NOT NULL
        , month int NOT NULL
        , year int NOT NULL
        , weekday int NOT NULL
    );
""")

# INSERT RECORDS



song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id)
    DO NOTHING;
""")


########### DEPRECATED #####################
# We use bulk insert with COPY statments in etl.py
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time)
    DO NOTHING;
""")

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) 
    DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id)
    DO NOTHING;
""")

############################################

# FIND SONGS

########### DEPRECATED #####################
# We use song_select_all instead to run
# a query once and merge data in Pandas
# in ETL.py to be able to do bulk inserts
song_select = ("""
    SELECT 
        s.song_id
        , s.artist_id
    FROM songs s
    JOIN artists a
        ON s.artist_id = a.artist_id
    WHERE 1=1
        AND s.title = %s
        AND a.name = %s
        AND s.duration = %s
""")

############################################

song_select_all = ("""
    SELECT
        s.title as song
        , a.name as artist
        , s.duration as length
        , s.song_id
        , s.artist_id
    FROM songs s
    JOIN artists a
        ON s.artist_id = a.artist_id
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
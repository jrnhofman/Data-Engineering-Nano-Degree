import os
from io import StringIO
import csv
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a song file

    Parameters
    ----------
    cur : cursor for current connection
    filepath : path of the song file
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except ValueError as e:
        print("Unable to open file: ", filepath)

    # insert song record, since it's single values at a time
    # we use single inserts
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record, since it's single values at a time
    # we use single inserts
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location'
                           , 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def psql_insert_copy(table, cur, df, conflict_col, conflict_action="DO NOTHING"):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    cur : cursor
    df : Input dataframe
    conflict_col : column which should be used to detect conflicts in
    insertion
    """

    s_buf = StringIO()
    writer = csv.writer(s_buf)
    writer.writerows(df.itertuples(index=False))
    s_buf.seek(0)
    columns = (str(df.columns.values.tolist())
                    .replace('[','').replace(']','').replace("'",""))

    sql = """
        CREATE TABLE temp_{table}(LIKE {table});
        COPY temp_{table} FROM STDIN WITH CSV;

        INSERT INTO {table} AS t ({columns})
        SELECT *
        FROM temp_{table} ON conflict ({conflict_col})
        {conflict_action};

        DROP TABLE temp_{table};
        """.format(conflict_col=conflict_col, table=table, columns=columns, conflict_action=conflict_action)
    cur.copy_expert(sql=sql, file=s_buf)


def process_log_file(cur, filepath):
    """
    Processes a log file

    Parameters
    ----------
    cur : cursor for current connection
    filepath : path of the log file
    """
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except ValueError as e:
        print("Unable to open file: ", filepath)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    time_data = [t.values, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({k:v for k,v in zip(column_labels,time_data)})

    psql_insert_copy('time', cur, time_df, 'start_time')

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.rename(axis=1,mapper={'userId':'user_id', 'firstName':'first_name', 'lastName':'last_name'})

    # sets the user level if the current level is 'free' to allow for upgrades from 'free' to 'paid'
    # we have to drop exact duplicates as this is a requirement for updates on conflict in PostgreSQL
    # we want to keep the user_id with a level which is the 'last' in alphabetical order, i.e. if we have
    # a user with both levels 'free' and 'paid' we want to take the 'paid' version
    user_df = user_df.sort_values(by=['user_id', 'level']).drop_duplicates(subset='user_id', keep='last')
    user_conflict_action = """DO UPDATE SET level = EXCLUDED.level WHERE t.level='free'"""

    psql_insert_copy('users', cur, user_df, 'user_id', user_conflict_action)

    # get artistid, songid for all songs and artists
    cur.execute(song_select_all)
    all_songs = cur.fetchall()

    all_songs_df = pd.DataFrame(all_songs, columns=['song', 'artist', 'length', 'song_id', 'artist_id'])

    # merge the log DF on song artist and length, all but one value
    # of the left join will be NaN on the right side
    songplay_df = df.merge(all_songs_df, on=['song', 'artist', 'length'], how='left')

    # Creating the right columns and renaming to new schema
    songplay_df['ts'] = pd.to_datetime(songplay_df['ts'], unit='ms')
    songplay_df['index'] = songplay_df.index.values
    old_column_names = ['index', 'ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    columns = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
    songplay_df = songplay_df.rename(mapper=dict(zip(old_column_names,columns)), axis=1)[columns]

    psql_insert_copy('songplays', cur, songplay_df, 'songplay_id')


def process_data(cur, conn, filepath, func):
    """
    Collects files to process and calls
    either process_{log|song}_file

    Parameters
    ----------
    cur : cursor for current connection
    conn : the connection
    filepath : path of the log file
    func : function that is called for each file, either
    process_log_file or process_song_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

# Project 1: Data Modeling with PostGres

Author: Jeroen Hofman
Updated: June 7, 2020

## Brief description:
Following instructions we built a star schema for songplay data from Sparkify. Starschemas are useful since we have a fact table (songplays) and then can get more detailed info using dimension tables by doing a series of simple joins. As such, these type of schemas are useful for querying analytical data (see below).

## Tables:
Fact table:
- songplays: contains unique entry per songplay, for the unique songplay_id we simply used an index.
Dimension tables:
- time: contains unique entry per timestamp and extracts time units from it
- songs: contains unique entry per song and describes the song in more detail
- artists: contains unique entry per artis and describes the artist in more detail
- users: contains unique entry per user and describes the user in more detail

- Each table has a primary key which is the identifier described above
- Each field in principle is considered to be non-null, however there are a few exceptions - namely artist + song id since we are only using part of the dataset. (normally you would not wants those fields to be NULL as that will give problems with joins). Other null fields are in the location data of the artist.
- Each table is deduplicated on the primary key by using 'ON CONFLICT({PRIMARY_KEY}) DO NOTHING' in the insert statements

## Data ingestion:
For data ingestion we choose the following set-up:
- files are read sequentially
- all entries within a file are processed in bulk

For song files, the implementation is trivial since we have only one row per file and we call simple insert statements defined in sql_queries.py.

For log files, we implemented a slightly different approach then what was proposed in the template, specifically we set it up as follows:
- We replaced calling an insert statement by using an iterable over the DF of the log file by creating a StringIO object and using a COPY statement with copy_expert in psycopg2 which bulk inserts the entire content of the DF (technically buffers the data in a streaming way, but in any case it is much faster than doing single inserts). Since psycopg2 doesn't support 'ON CONFLICT' clauses directly we first ingest data to a temporary table and then insert in the final table using the ON CONFLICT clause.
- To use the above, we need to do a bit more preprocessing in Pandas so that the insert is a simple insert. Most notably, we replaced the query to get artist + song id per songplay with a general query that gets all songs and artists with their IDs. We then join the log DF file to this query result to obtain artist + song ids for all entries in the log file at once.

The first point above is implemented in psql_insert_copy in etl.py. The second is an adaptation of process_log_file where the for loops over iterables are replaced by calls to psql_insert_copy after doing some initial data manipulation.

The file sql_queries.py has now several deprecated functions which are clearly marked in the file.
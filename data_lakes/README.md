# Project 4: Data Lakes

Author: Jeroen Hofman
Updated: June 29, 2020

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

- Tables are created using spark.sql where possible, for convenience and to keep in line with earlier exercises
- Each table is deduplicated using the spark native 'drop_duplicates' function.

## Data ingestion:
Files from the S3 bucket are fetched using a regex, processed in spark and then are dumped into my private S3 bucket.


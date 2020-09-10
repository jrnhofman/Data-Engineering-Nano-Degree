# Project 3: Data Warehouse in AWS

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

- Tables are created using 2 staging tables, which are dumps of the json files in the S3 bucket
- Since we expect joins from the songplay table on the user table to be fairly common we use the user_id as a distribution column. Alternatively we could have chosen the artist_id or something similar but since we don't have a full view of the dataset it's hard to say what's better. We also sort each of the tables for faster joins, on what was considered the primary key in postgresql.
- Each field in principle is considered to be non-null, however there are a few exceptions - namely artist + song id since we are only using part of the dataset. (normally you would not wants those fields to be NULL as that will give problems with joins). Other null fields are in the location data of the artist. We don't apply non null constraints on the staging tables as they are an unfiltered dump of the json files and hence can contain nulls in other fields (in rows which we filter out for our tables later).
- Each table is deduplicated using an insert where the row is unique by using the 'distinct' clause.

## Data ingestion:
Files from the S3 bucket are dumped into 2 staging tables, which basically just copy the data plainly into RedShift.

From there, the tables are populated, where the songplay table is a result of the join of the 2 staging tables. All queries and accompanying create and delete statements can be found in sql_queries.py.

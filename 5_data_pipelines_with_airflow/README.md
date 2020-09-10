# Project 4: Data Pipelines in Airflow

Author: Jeroen Hofman
Updated: August 30, 2020

### Technologies:
Airflow, Python, SQL

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
- From the staging files the dimension and fact tables are created.

## Project specifics:
- We added a step where we are creating tables as the staging part doesn't take care of this by itself. For this an operator called 'CreateTablesOperator' is used.
- For the data quality check we are checking counts of tables that we created, as well as checking if some fields contain NULLS.
- The dimension table has an option to append or overwrite the table, by default we will overwrite.

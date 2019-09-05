# P1.A: Data Modeling with Postgres

This project comprises the scripts required for building a database (using a *star schema*) and the ETL processes, for a fictitious startup called Sparkify. This company had been collecting data on user activity from their music streaming application, and storing them as JSON files. However, this rudimentary way of storing data generated some difficulties for them regarding performing queries and extracting insights from the data.

During this project, a Postgres database and relevant tables are setup, to allow the Sparkify analytics team access, aggregate and generate insights from their users’ data.

## Database design

For building the database I selected a star schema, as it simplifies generating queries and can perform fast aggregations. The fact and dimension table are built as follows:


![Database schema](database_schema.jpg)


### Fact Table

- Songplays: records in log data associated with song plays. Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

- Users: users in the app. Columns: user_id (PK), first_name, last_name, gender, level
- Songs: songs in music database. Columns: song_id (PK), title, artist_id, year, duration
- Artists: artists in music database. Columns: artist_id (PK), name, location, latitude, longitude
- Time: timestamps of records in songplays broken down into specific units. Columns: start_time (PK), hour, day, week, month, year, weekday

## How to use

First, make sure you have postgresql installed. After that, to generate and populate the tables, go to the terminal and execute the following in the repository path:

```sh
python create_tables.py
python etl.py
```

This will result in the fact and dimensions tables as specified previously. Check the section below for some sample queries!


## Sample queries

To get started, here are some queries that could be of use for understanding the behavior of users:

```sql
-- Check most popular artists
SELECT name as artist_name, count(songplay_id)
FROM songplays s
JOIN artists a ON s.artist_id = a.artist_id
GROUP BY name;

-- Check average length of songs listened by users
SELECT user_id, avg(duration) 
FROM songplays s 
JOIN songs o ON s.song_id = o.song_id 
GROUP BY user_id;
```


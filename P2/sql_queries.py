import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist        VARCHAR,
auth          VARCHAR,
firstName     VARCHAR,
gender        VARCHAR,
itemInSession INTEGER,
lastName      VARCHAR,
length        DECIMAL,
level         VARCHAR,
location      VARCHAR,
method        VARCHAR,
page          VARCHAR,
registration  VARCHAR,
sessionId     INTEGER,
song          TEXT,
status        INTEGER,
ts            TIMESTAMP,
userAgent     TEXT,
userId        INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs        INTEGER,
artist_id        TEXT,
artist_latitude  FLOAT,
artist_longitude FLOAT,
artist_location  VARCHAR,
artist_name      TEXT,
song_id          TEXT,
title            TEXT,
duration         FLOAT,
year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time  TIMESTAMP NOT NULL,
user_id     INTEGER NOT NULL,
level       VARCHAR,
song_id     TEXT NOT NULL,
artist_id   TEXT NOT NULL,
session_id  INTEGER NOT NULL,
location    VARCHAR,
user_agent  TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id    INTEGER IDENTITY(0,1) PRIMARY KEY, 
first_name TEXT,
last_name  TEXT,
gender     TEXT,
level      TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id   TEXT PRIMARY KEY,
title     TEXT,
artist_id TEXT NOT NULL,
year      INTEGER,
duration  FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id TEXT PRIMARY KEY,
name      TEXT,
location  TEXT,
latitude  FLOAT,
longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY, 
hour       INTEGER NOT NULL, 
day        INTEGER NOT NULL, 
week       INTEGER NOT NULL, 
month      INTEGER NOT NULL, 
year       INTEGER NOT NULL, 
weekday    INTEGER NOT NULL
);
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events 
FROM {}
IAM_ROLE {}
JSON {}
REGION 'us-west-2'
COMPUPDATE OFF
TIMEFORMAT AS 'epochmillisecs'
BLANKSASNULL EMPTYASNULL;
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
IAM_ROLE {}
JSON 'auto'
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  e.ts            AS start_time,
        e.userId        AS user_id,
        e.level         AS level,
        sa.song_id      AS song_id,
        sa.artist_id    AS artist_id,
        e.sessionId     AS session_id,
        e.location      AS location,
        e.userAgent     AS user_agent
FROM staging_events e
INNER JOIN (
    SELECT s.song_id, s.artist_id, s.title, a.name
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
    WHERE s.song_id IS NOT NULL AND a.artist_id IS NOT NULL
) sa ON e.song = sa.title AND e.artist = sa.name
WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (first_name, last_name, gender, level)
SELECT  DISTINCT
        firstName AS first_name,
        lastName  AS last_name,
        gender    AS gender,
        level     AS level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id is NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT
        artist_id,
        artist_name      AS name,
        artist_location  AS location,
        artist_latitude  AS latitude,
        artist_longitude AS longitude
FROM staging_songs
WHERE artist_id is NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT 
        ts                     AS start_time,
        EXTRACT(hour FROM ts)  AS hour,
        EXTRACT(day FROM ts)   AS day,
        EXTRACT(week FROM ts)  AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts)  AS year,
        EXTRACT(dow FROM ts)   AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [time_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

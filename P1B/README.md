# P1.B: Data Modeling with Apache Cassandra

This project consists of a notebook that processes data and generates a NoSQL database using Apache Cassandra, for a fictitious startup called Sparkify. The case study was that this company had been collecting data on user activity from their music streaming application and stored it as CSV files but could not query the data and generate insights out of it.

## Database design

Aside from the keyspace, 3 tables were generated to allow Sparkify fulfill different goals:

1. **sessions_history:** Return the artist, song title and song's length in the music app history that was heard for a specific session and item in session. 
2. **users_sessions:** Return the name of artist, song (sorted by itemInSession) and user (first and last name) for a specific user and session (e.g. 
3. **users_per_song:** Return every user name (first and last) in the music app history who listened to a specific song (e.g. All Hands Against His Own)

## Usage

Clone the repo to a local directory, and launch the notebook. Make sure you have the required packages specified in the import section of the notebook.

## Sample queries

For each of the tables and goals specified above, there are some sample queries you could execute:

1. **sessions_history:**
```sql
SELECT artist, song_title, length from sessions_history where session_id=338 and item_in_session=4;
```
2. **users_sessions:**
```sql
SELECT artist, song_title, user_first_name, user_last_name from users_sessions where user_id=10 and session_id=182;
```
3. **users_per_song:** 
```sql
SELECT user_first_name, user_last_name from users_per_song where song_title='All Hands Against His Own';
```

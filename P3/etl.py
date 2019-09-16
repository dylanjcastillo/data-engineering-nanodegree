import os
import configparser
from datetime import datetime
from tables_schemas import staging_events_schema, staging_songs_schema
from utils import get_timestamp

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark session for processing data"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song metadata files to generate songs and artists tables
    
    Parameters
    ----------
    spark
        Spark session in use
    input_data
        Location of input data
    output_data
        Path for generating output
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # read song data file
    staging_songs_df = spark.read.json(song_data, schema=staging_songs_schema)

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = staging_songs_df.select(song_cols)
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs")
    songs_table.write.parquet(songs_output, partitionBy=["year", "artist_id"], mode="overwrite")

    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = staging_songs_df.select(artists_cols)
    
    # write artists table to parquet files
    artists_output = os.path.join(output_data, "artists")
    artists_table.write.parquet(artists_output, mode="overwrite")

def process_log_data(spark, input_data, output_data):
    """Process log files to generate time, users, and songplays tables
    
    Parameters
    ----------
    spark
        Spark session in use
    input_data
        Location of input data
    output_data
        Path for generating output
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*.json")
    
    # read log data file
    staging_events_df = spark.read.json(log_data, schema=staging_events_schema)
    
    # filter by actions for song plays
    filtered_df = staging_events_df.filter("page == 'NextSong'")

    # extract columns for users table 
    users_cols = ["userId as user_id", 
                  "firstName as first_name", 
                  "lastName as last_name", 
                  "gender", 
                  "level"]
    users_table = filtered_df.selectExpr(*users_cols).distinct()
    
    # write users table to parquet files
    users_output = os.path.join(output_data, "users")
    users_table.write.parquet(users_output, mode="overwrite")

    # create timestamp column from original timestamp column
    filtered_df = filtered_df.withColumn("processed_ts", get_timestamp("ts"))
            
    # extract columns to create time table
    time_cols = ["processed_ts as start_time", 
                 "hour(processed_ts) as hour", 
                 "day(processed_ts) as day", 
                 "weekofyear(processed_ts) as week",
                 "month(processed_ts) as month",
                 "year(processed_ts) as year",
                 "weekday(processed_ts) as weekday"]
    time_table = filtered_df.selectExpr(*time_cols).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time")
    time_table.write.parquet(time_output, partitionBy=["year", "month"], mode="overwrite")

    # read in song and artists data to use for songplays table
    songs_input = os.path.join(output_data, "songs")
    artists_input = os.path.join(output_data, "artists")
    songs_df = spark.read.parquet(songs_input)
    artists_df = spark.read.parquet(artists_input)

    # extract columns from joined song and log datasets to create songplays table
    songplays_cols = ["processed_ts as start_time", 
                      "userId as user_id",
                      "level",
                      "song_id",
                      "artist_id",
                      "sessionId as session_id",
                      "location",
                      "userAgent as user_agent",
                      "year",
                      "month"]
    songplays_table = (
        filtered_df
        .join(songs_df, filtered_df.song == songs_df.title)
        .drop("artist_id")
        .join(artists_df, filtered_df.artist == artists_df.artist_name)
        .join(time_table, filtered_df.processed_ts == time_table.start_time)
        .filter("song_id is not null and artist_id is not null")
        .selectExpr(*songplays_cols)
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, "songplays")
    songplays_table.write.parquet(songplays_output, partitionBy=["year", "month"], mode="overwrite")

def main():
    import subprocess
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

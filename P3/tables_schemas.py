from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType

staging_events_schema = StructType(
    [
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", FloatType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", StringType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", DoubleType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ]
)

staging_songs_schema = StructType(
    [
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", FloatType(), True),
        StructField("artist_longitude", FloatType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", FloatType(), True),
        StructField("year", IntegerType(), True)
    ]
)
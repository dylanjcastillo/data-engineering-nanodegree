import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

@udf(TimestampType())
def get_timestamp(ms):
    """Generate timestamp from milliseconds"""
    return datetime.datetime.fromtimestamp(ms / 1000)


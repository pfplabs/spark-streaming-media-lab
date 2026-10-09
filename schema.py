from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType)

PLAYBACK_SCHEMA = StructType([
    StructField("viewer_id", IntegerType(), nullable=False),
    StructField("title_id", IntegerType(), nullable=False),
    StructField("device", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("watch_seconds", IntegerType(), nullable=True),
    StructField("_corrupt_record", StringType(), nullable=True),
])

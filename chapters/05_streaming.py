import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import spark_session, path
from data import generate_events, catalog as make_catalog
from pyspark.sql import functions as F, Window
from schema import PLAYBACK_SCHEMA
from transforms import sessionize
from quality import apply_checks
spark = spark_session()
events = generate_events(spark, int(__import__("os").environ.get("N_EVENTS", "200000")))
catalog = make_catalog(spark)

import argparse
p = argparse.ArgumentParser()
p.add_argument("--seconds", type=int, default=45)
a = p.parse_args()
stream = (
    spark.readStream.format("rate").option("rowsPerSecond", 200).load()
    .withColumn("viewer_id", (F.col("value") % 2_000).cast("int"))
    .withColumn("title_id", (F.col("value") % 300).cast("int"))
    .withColumn("event_type", F.when(F.col("value") % 5 == 0, "stop").otherwise("heartbeat"))
    # up to 90 seconds of simulated lateness
    .withColumn("event_ts", F.col("timestamp") - F.expr("make_interval(0,0,0,0,0,0, rand() * 90)"))
)
concurrent = (
    stream.filter(F.col("event_type") == "heartbeat")
    .withWatermark("event_ts", "2 minutes")
    .groupBy(F.window("event_ts", "1 minute"), "title_id")
    .agg(F.approx_count_distinct("viewer_id").alias("concurrent_viewers"))
)

query = (
    concurrent.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", path("_chk/concurrency"))
    .trigger(processingTime="30 seconds")
    .start(path("gold_concurrency"))
)
try:
    query.awaitTermination(a.seconds)
    print(query.lastProgress)
finally:
    query.stop()

spark.stop()

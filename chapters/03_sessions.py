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

good = events
from pyspark.sql import Window

by_viewer = Window.partitionBy("viewer_id").orderBy("event_ts")
GAP = 30 * 60

sessions = (
    good
    .withColumn("prev_ts", F.lag("event_ts").over(by_viewer))
    .withColumn("new_session", F.when(
        F.col("prev_ts").isNull() |
        (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long") > GAP), 1
    ).otherwise(0))
    .withColumn("session_n", F.sum("new_session").over(
        by_viewer.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("session_id", F.concat_ws("-", "viewer_id", "session_n"))
)
session_titles = (
    sessions.groupBy("session_id", "viewer_id", "title_id")
    .agg(F.sum("watch_seconds").alias("watched"),
         F.max(F.col("event_type") == "complete").alias("completed"))
    .join(F.broadcast(catalog), "title_id")
)

genre_metrics = (
    session_titles.groupBy("genre")
    .agg(F.countDistinct("viewer_id").alias("viewers"),
         F.round(F.avg(F.col("completed").cast("double")), 3).alias("completion_rate"),
         F.round(F.sum("watched") / 3600, 1).alias("hours"))
    .orderBy(F.desc("hours"))
)
by_genre = Window.partitionBy("genre").orderBy(F.desc("hours"))
top_titles = (
    session_titles.groupBy("genre", "title_id")
    .agg((F.sum("watched") / 3600).alias("hours"))
    .withColumn("rank", F.dense_rank().over(by_genre))
    .filter("rank <= 3")
)
genre_metrics.show()
top_titles.show()
spark.stop()

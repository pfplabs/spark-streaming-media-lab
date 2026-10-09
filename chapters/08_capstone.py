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

from delta.tables import DeltaTable
from transforms import sessionize
from quality import apply_checks

bronze = spark.read.format("delta").load(path("bronze_playback"))
valid, rejected = apply_checks(bronze)
failure_rate = rejected.count() / max(bronze.count(), 1)
assert failure_rate < 0.01, f"Quality gate failed: {failure_rate:.2%}"

silver_batch = (
    sessionize(valid.dropDuplicates(["viewer_id", "title_id", "event_type", "event_ts", "watch_seconds"]))
    .withColumn("event_key", F.sha2(F.concat_ws("|",
        "viewer_id", "title_id", "event_type", "event_ts", "watch_seconds"), 256))
    .join(F.broadcast(catalog), "title_id")
)

SILVER = path("silver_sessions")
if not DeltaTable.isDeltaTable(spark, SILVER):
    silver_batch.limit(0).write.format("delta").save(SILVER)

(DeltaTable.forPath(spark, SILVER).alias("t")
    .merge(silver_batch.alias("s"), "t.event_key = s.event_key")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
gold = (
    spark.read.format("delta").load(SILVER)
    .withColumn("day", F.to_date("event_ts"))
    .groupBy("day", "genre")
    .agg(F.countDistinct("viewer_id").alias("viewers"),
         F.round(F.sum("watch_seconds") / 3600, 1).alias("hours"),
         F.round(F.avg((F.col("event_type") == "complete").cast("double")), 3)
          .alias("completion_event_share"))
)

(gold.write.format("delta").mode("overwrite")
    .save(path("gold_genre_daily")))
gold.show()
print("silver rows", spark.read.format("delta").load(SILVER).count())

spark.stop()

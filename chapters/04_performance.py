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

joined = events.join(F.broadcast(catalog), "title_id")
joined.explain()
skewed = events.withColumn("title_id",
    F.when(F.rand(seed=7) < 0.4, F.lit(42)).otherwise(F.col("title_id")))

# A salted aggregation spreads the hot key across buckets
SALT = 16
salted = (
    skewed.withColumn("salt", (F.rand(seed=1) * SALT).cast("int"))
    .groupBy("title_id", "salt").agg(F.sum("watch_seconds").alias("s"))
    .groupBy("title_id").agg(F.sum("s").alias("watch_seconds"))
)
salted.show()
spark.stop()

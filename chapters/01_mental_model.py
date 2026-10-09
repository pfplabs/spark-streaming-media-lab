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

completions_by_device = (
    events.filter(F.col("event_type") == "complete")
    .groupBy("device")
    .agg(F.count("*").alias("completions"),
         F.round(F.avg("watch_seconds") / 60, 1).alias("avg_minutes"))
    .orderBy(F.desc("completions"))
)

completions_by_device.explain(mode="formatted")  # still lazy
completions_by_device.show()                      # action: runs now
spark.stop()

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

# Local verification target for the Genie Code review exercise.
# Genie Code itself requires a Databricks workspace; no local AI output is claimed.
from pyspark.testing import assertDataFrameEqual
from datetime import datetime
fixture = spark.createDataFrame([(1, datetime(2026,10,1,20,0)), (1, datetime(2026,10,1,20,30)), (1, datetime(2026,10,1,21,1))], "viewer_id int, event_ts timestamp")
assert [r.session_n for r in sessionize(fixture).orderBy("event_ts").collect()] == [1,1,2]
print("Review fixture passed; use chapters/07_genie_code.md for workspace steps.")

spark.stop()

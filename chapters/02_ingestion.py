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

RAW = path("raw_events")
events.write.mode("overwrite").json(RAW)

# Add a few deliberately broken lines
spark.createDataFrame(
    [('{"viewer_id": "not-a-number", "event_type": "start"}',),
     ('{broken json',)], ["value"]
).write.mode("append").text(RAW)
raw = (
    spark.read.schema(PLAYBACK_SCHEMA)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(RAW)
    .withColumn("_source_file", F.input_file_name())
    .cache()  # required before filtering on the corrupt column alone
)

bad = raw.filter(F.col("_corrupt_record").isNotNull())
good = raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
print(good.count(), bad.count())
bronze = (
    good.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_source_file"))
)

(bronze.write.format("delta")
    .mode("append")
    .partitionBy("device")
    .save(path("bronze_playback")))

bad.write.mode("append").json(path("quarantine"))
spark.stop()

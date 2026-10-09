from pyspark.sql import DataFrame, Window, functions as F

def sessionize(df: DataFrame, gap_seconds: int = 1800) -> DataFrame:
    w = Window.partitionBy("viewer_id").orderBy("event_ts", *[c for c in ["title_id", "event_type", "watch_seconds"] if c in df.columns])
    return (
        df.withColumn("prev_ts", F.lag("event_ts").over(w))
          .withColumn("new_session", F.when(
              F.col("prev_ts").isNull() |
              (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long") > gap_seconds), 1
          ).otherwise(0))
          .withColumn("session_n", F.sum("new_session").over(
              w.rowsBetween(Window.unboundedPreceding, 0)))
          .drop("prev_ts", "new_session")
    )

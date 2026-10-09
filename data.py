from pyspark.sql import functions as F

def generate_events(spark, n=200_000):
    N_EVENTS = n
    events = (
        spark.range(N_EVENTS)
        .withColumn("viewer_id", (F.col("id") % 5_000).cast("int"))
        .withColumn("title_id", (F.hash("id") % 300).cast("int"))
        .withColumn("title_id", F.abs("title_id"))
        .withColumn("device", F.element_at(
            F.array(*[F.lit(d) for d in ["tv", "mobile", "web", "tablet"]]),
            (F.col("id") % 4 + 1).cast("int")))
        .withColumn("event_type", F.element_at(
            F.array(*[F.lit(e) for e in ["start", "pause", "resume", "complete"]]),
            (F.abs(F.hash("id", F.lit("e"))) % 4 + 1).cast("int")))
        .withColumn("event_ts", F.timestamp_seconds(
            F.lit(1_790_000_000) + F.col("id") * 7))
        .withColumn("watch_seconds",
            (F.abs(F.hash("id", F.lit("w"))) % 3_600).cast("int"))
        .drop("id")
    )
    return events

def catalog(spark):
    catalog = (
        spark.range(300).withColumnRenamed("id", "title_id")
        .withColumn("title_id", F.col("title_id").cast("int"))
        .withColumn("genre", F.element_at(
            F.array(*[F.lit(g) for g in ["drama", "comedy", "docs", "kids", "thriller"]]),
            (F.col("title_id") % 5 + 1).cast("int")))
        .withColumn("runtime_minutes", (F.col("title_id") % 90 + 22).cast("int"))
    )
    return catalog

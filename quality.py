from pyspark.sql import functions as F
def apply_checks(df):
    CHECKS = {
        "viewer_id_present": F.col("viewer_id").isNotNull(),
        "known_event_type": F.col("event_type").isin("start", "pause", "resume", "complete"),
        "watch_in_range": F.col("watch_seconds").between(0, 6 * 3600),
    }
    
    flagged = df
    for name, cond in CHECKS.items():
        flagged = flagged.withColumn(f"ok_{name}", F.coalesce(cond, F.lit(False)))
    ok = F.expr(" AND ".join(f"ok_{n}" for n in CHECKS))
    return flagged.filter(ok), flagged.filter(~ok)



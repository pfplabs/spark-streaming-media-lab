from datetime import datetime
from pyspark.testing import assertDataFrameEqual
from transforms import sessionize

def test_gap_starts_new_session(spark):
    t = lambda m: datetime(2026, 10, 1, 20, m)
    df = spark.createDataFrame(
        [(1, t(0)), (1, t(10)), (1, t(55)), (2, t(0))],
        "viewer_id int, event_ts timestamp")

    got = sessionize(df).select("viewer_id", "event_ts", "session_n")
    expected = spark.createDataFrame(
        [(1, t(0), 1), (1, t(10), 1), (1, t(55), 2), (2, t(0), 1)],
        "viewer_id int, event_ts timestamp, session_n bigint")

    assertDataFrameEqual(got, expected)

def test_exact_gap_and_ties(spark):
    df = spark.createDataFrame([(1, datetime(2026,10,1,20,0)), (1, datetime(2026,10,1,20,0)), (1, datetime(2026,10,1,20,30)), (1, datetime(2026,10,1,21,1))], "viewer_id int, event_ts timestamp")
    assert [r.session_n for r in sessionize(df).orderBy("event_ts").collect()] == [1,1,1,2]

def test_quality_null_and_invalid(spark):
    from quality import apply_checks
    df = spark.createDataFrame([(1,"start",0),(None,"start",1),(2,"oops",1),(3,"pause",-1)], "viewer_id int, event_type string, watch_seconds int")
    valid, bad = apply_checks(df)
    assert valid.count() == 1
    assert bad.count() == 3

def test_generator(spark):
    from data import generate_events
    a = generate_events(spark,100)
    assert a.count() == 100
    assertDataFrameEqual(a, generate_events(spark,100))

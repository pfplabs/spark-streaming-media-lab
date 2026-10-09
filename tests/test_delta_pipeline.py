from pyspark.sql import functions as F
from delta.tables import DeltaTable
from data import generate_events, catalog
from schema import PLAYBACK_SCHEMA
from transforms import sessionize

def test_malformed_ingestion_and_silver_rerun(spark, tmp_path):
    raw_path = str(tmp_path / 'raw')
    generate_events(spark, 20).write.json(raw_path)
    spark.createDataFrame([('{broken json',), ('{"viewer_id":"oops"}',)], ['value']).write.mode('append').text(raw_path)
    raw = spark.read.schema(PLAYBACK_SCHEMA).option('columnNameOfCorruptRecord','_corrupt_record').json(raw_path).cache()
    assert raw.count() == 22
    assert raw.filter(F.col('_corrupt_record').isNotNull()).count() == 2
    good = raw.filter(F.col('_corrupt_record').isNull()).drop('_corrupt_record')
    payload = ['viewer_id','title_id','event_type','event_ts','watch_seconds']
    batch = sessionize(good.unionByName(good).dropDuplicates(payload)).withColumn('event_key', F.sha2(F.concat_ws('|',*payload),256)).join(F.broadcast(catalog(spark)),'title_id')
    target = str(tmp_path / 'silver')
    batch.limit(0).write.format('delta').save(target)
    for _ in range(2):
        DeltaTable.forPath(spark,target).alias('t').merge(batch.alias('s'),'t.event_key = s.event_key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        assert spark.read.format('delta').load(target).count() == 20
    raw.unpersist()

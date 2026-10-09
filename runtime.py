from pathlib import Path
import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
ROOT = Path(os.environ.get("MEDIA_LAB_ROOT", ".lab")).resolve()
ROOT.mkdir(parents=True, exist_ok=True)
def path(name): return str(ROOT / name)
def spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (SparkSession.builder.master("local[2]").appName("media-lab")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

import os
import sys
from pathlib import Path

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PATH_JARS = Path(__file__).absolute().parent / 'jars'


def get_spark(app_name: str = "spark_training_spark3", master: str = "local[2]") -> SparkSession:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    spark_conf = pyspark.SparkConf().setAppName(app_name).setMaster(master)
    spark_conf = spark_conf.set("spark.jars", ','.join(map(str, PATH_JARS.glob('*.jar'))))
    spark_conf = spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark_conf = spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(SparkSession.builder.config(conf=spark_conf)).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    assert (spark.sparkContext.pythonVer == "3.10")
    print(spark.sparkContext.uiWebUrl)
    return spark

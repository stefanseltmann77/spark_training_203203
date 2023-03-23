# %% SETUP #############################################################################################################
from pyspark.sql import functions as sf
from pyspark.sql.utils import AnalysisException

from spark_setup_spark3 import get_spark
import pandas as pd

spark = get_spark()

if True:
    spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Spark3.2

spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")   # raise if too low
spark.conf.set("spark.sql.adaptive.enabled", "true")  # make, sure it is enabled

df = spark.read.csv("./data/recipeData.csv", inferSchema=True, header=True, encoding="utf8")
df_styles = spark.read.csv("./data/styleData.csv", inferSchema=True, header=True, encoding="utf8")
df.printSchema()
df_styles.printSchema()
df_styles.show()

from pyspark.sql import  functions as sf

df_joined = df.join(df_styles, on="StyleID", how="left")

df_joined.collect()

df_joined.repartition(10, 'StyleID')









# try to set the broadcast deliberately to make your code more explicit.
df_joined = df.join(sf.broadcast(df_styles), on="StyleID", how="left")
df_joined.explain()
df_joined.count()


# -> with Spark3.+ you should get broadcast joins automatically due to AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")  # make, sure it is enabled
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100000")   # legacy, raise if too low
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "100000")   # raise if too low

df_joined = df.join(df_styles, on="StyleID", how="left")
df_joined.explain()
df_joined.count()


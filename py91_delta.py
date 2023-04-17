# %% SETUP #############################################################################################################
import datetime
import delta
import os
import pyspark.sql.functions as sf
from delta.tables import DeltaTable
from pathlib import Path
from pprint import pprint
from py4j.protocol import Py4JJavaError

from spark_setup_spark3 import get_spark

PATH = "delta/recipe_2023"

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

# %% Use Delta right away ##############################################################################################
# simply safe the table to the new format

df.show()

df.write. \
    format("delta"). \
    save(PATH, mode='overwrite')

# every_time you write new data the old will be kept
df.write.format("delta").save(PATH, mode='overwrite')
df.write.format("delta").save(PATH, mode='overwrite')
df.write.format("delta").save(PATH, mode='overwrite')

# %% how can I check the revisions? ####################################################################################

# register the table
deltatab = DeltaTable.forPath(spark, PATH)

deltatab.history()

# show me version history
deltatab.history().show(truncate=False)
# or just the most recent one
deltatab.history(1).show()
# show the operational metrics
deltatab.history().select("operationMetrics").show(truncate=False)

df = spark.read.format('delta').load(PATH)
df.count()

# %% ACID: Read and write from the same table: #########################################################################
# What if we read and write from the same parquet table.
df_csv = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
df_csv.write.parquet('tmp/parquet', mode='overwrite')

df_parquet = spark.read.parquet('tmp/parquet')
try:
    df_parquet.write.parquet('tmp/parquet', mode='overwrite')
except Py4JJavaError:
    print("Reading and writing from the same parquet is not allowed!")


# Let's try the same with delta
df = spark. \
    read. \
    format("delta"). \
    load(PATH)
df.write.format("delta").save(PATH, mode='append')
# works without error

# %% Reading Delta as Parquet ##########################################################################################
df_csv = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
tmp_delta_path = 'tmp/delta'

df_csv.write.format('delta').save(tmp_delta_path, mode='overwrite')
df_csv.write.format('delta').save(tmp_delta_path, mode='overwrite')

try:
    assert spark.read.format("parquet").load(path=tmp_delta_path).count() == \
           spark.read.format("delta").load(path=tmp_delta_path).count()
except AssertionError:
    print("Assertion error, because reading delta as plain parquet, will also read deleted rows.")
deltatab = DeltaTable.forPath(spark, tmp_delta_path)


# %% load the data depending on time or version ########################################################################

# based on the version number
df = spark. \
    read. \
    format("delta"). \
    option("versionAsOf", 0). \
    load(PATH)

# based on the time => timetravel
df = spark. \
    read. \
    format("delta"). \
    option("timestampAsOf", '2023-03-31 07:40:00'). \
    load(PATH)

# cleanup of old versions
deltatab.history().show()
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
deltatab.vacuum(retentionHours=0)
spark.sparkContext.uiWebUrl

# %% try deletes and updates

deltatab.delete(sf.col("BrewMethod") == "extract")
deltatab.update(sf.col("BrewMethod") == "BIAB", {"BrewMethod": sf.lit("B.I.A.B")})

# you can see the update
deltatab.history().show(truncate=False)


# %% Merging and updating complete tables

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")


df_initial = df.sample(withReplacement=False, fraction=0.5, seed=42)
df_update = df.sample(withReplacement=False, fraction=0.5, seed=21)

df_initial.write.format("delta").save("delta/recipe_merge", mode='overwrite')

df_initial.count()
df_update.count()

deltadf_merge = DeltaTable.forPath(spark, "delta/recipe_merge")


# starting conditions
deltadf_merge.toDF().count()  # 37055
df_update.count()  # 36827

deltadf_merge.alias("root").\
    merge(source=df_update.alias("updates"),
          condition="root.BeerID == updates.BeerID").\
    whenNotMatchedInsertAll().\
    execute()

# merged count as aspected lower as the sum of both.
deltadf_merge.toDF().count()  # 55580

deltadf_merge.alias("root").\
    merge(source=df_update.alias("updates"),
          condition="root.BeerID == updates.BeerID").\
    whenNotMatchedInsertAll().\
    execute()

# count is unchanged after second merge
deltadf_merge.toDF().count()  # 55580
deltadf_merge.history().show(truncate=False)


# %% SCD2
df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")

PATH_SCD2 = 'delta/beers_scd2'

df_initial = df.sample(withReplacement=False, fraction=0.5, seed=42)
df_update = df.sample(withReplacement=False, fraction=0.5, seed=21)

valid_to_dts_max = datetime.datetime(2199, 12, 31)


df_initial = df_initial. \
    withColumn("valid_from_dts", sf.lit(datetime.datetime.now())). \
    withColumn("valid_to_dts", sf.lit(valid_to_dts_max))
df_initial.write.format("delta").save(PATH_SCD2, mode='overwrite')
df_initial.show()

deltadf_scd2 = DeltaTable.forPath(spark, PATH_SCD2)
deltadf_scd2.toDF().columns
update_dts = sf.lit(datetime.datetime.now())

df_update.union(df_update)

deltadf_scd2.alias("root"). \
    merge(source=df_update. \
          withColumn("join_id", sf.lit(None)). \
          withColumn("valid_from_dts", update_dts). \
          withColumn("valid_to_dts", sf.lit(valid_to_dts_max)). \
          union(df_update.
                withColumn("join_id", sf.col("BeerID")).
                withColumn("valid_from_dts", sf.lit(None)).
                withColumn("valid_to_dts", sf.lit(None))).alias("updates"),
          condition="root.BeerID = updates.join_id and root.valid_to_dts= '2199-12-31'"). \
    whenMatchedUpdate(set={'valid_to_dts': update_dts}). \
    whenNotMatchedInsertAll(). \
    execute()

deltadf_scd2.toDF().where('BeerID = 21').show(truncate=False)

# %% Optimize #########################################################################################################

df = spark.read.option("encoding", "utf8") \
    .csv("./data/recipeData.csv",
         inferSchema=True,
         header=True).withColumnRenamed("Size(L)", "Size_L")
PATH_FRAGMENTED = 'delta/fragemented'

# fragment a dataset into 100 chunks
df.repartition(100).write.format('delta').save(path=PATH_FRAGMENTED, mode='overwrite')

# check the number of files
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))
# way to much!!!

# compact the files again within a partition
table = delta.DeltaTable.forPath(spark, PATH_FRAGMENTED)
table.optimize().executeCompaction()

# check number of files again [... still too many]
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))

# cleanup old versions
table.vacuum(0)

# now only small number of files
pprint(os.listdir(PATH_FRAGMENTED))
pprint(len(os.listdir(PATH_FRAGMENTED)))

# %% Optimize ZOrdering ################################################################################################

table = delta.DeltaTable.forPath(spark, PATH_FRAGMENTED)
table.optimize().executeZOrderBy('BeerID')  # physically order by BeerId

# can also be done if partitioned.
# table.optimize().where("BrewMethod='All Grain'").executeZOrderBy('BeerID')

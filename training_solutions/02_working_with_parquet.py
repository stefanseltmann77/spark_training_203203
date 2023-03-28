from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise: Save the data as a plain parquet file.
# Lookup the number of resulting files in the filesystem.
df.write.parquet("books_plain", mode="overwrite")

# %% Exercise: Save the data partitioned by language, choose a different name.
# Explore the folder containing the data. How does the file structure look like?
# how many files are there now?
df.write.parquet("books_by_lang", partitionBy="language_code",
                 mode="overwrite")

# %% Exercise: Read the partitioned parquet into a new DataFrame.
df_part = spark.read.parquet("books_by_lang")

# %% Exercise: Print the schema.
df_part.printSchema()

# %% Exercise: Save the file again
# distributed by language in 5 partitions.
df_part.repartition(5, "language_code").write.parquet("books5", mode="overwrite")
# or using the hive metastore
df_part.repartition(5, "language_code").write.saveAsTable("books5", mode="overwrite")

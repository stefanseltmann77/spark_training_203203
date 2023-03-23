from pyspark.sql.types import StringType

from spark_setup_spark3 import get_spark
import pyspark.sql.functions as sf

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

import pandas as pd

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

# %% Exercise 01: How many rows of data are there?
df.count()

# %% Exercise 02: What columns are there and which datatypes?
df.columns
for row in df.dtypes:
    print(row)
df.printSchema()

# %% Exercise 03: cast the zipcodeOri to string
df = df.withColumn('zipcodeOri', df.zipcodeOri.cast(StringType()))
df.printSchema()


# %% Exercise 04: Build a new flag (0/1) that signifies if the customer is female
df.select(df.gender).show()

df = df.withColumn('is_female', sf.when(df.gender=='F', 1).otherwise(0))
df.show()

# %% Exercise 05: How many distinct customers are there?

df.select(sf.count_distinct(df.customer)).show()  # 4112

# %% Exercise 06: How many fraud cases are there?

df.select(sf.sum(df.fraud)).show()  # 7200
df.where(df.fraud == 1).count()  # 7200


# %% Exercise 07: How many distinct female fraud customers are there. Use the new flag for is_female?

df.where(df.fraud == 1).\
    where(df.is_female == 1).\
    select(sf.count_distinct(df.customer)).show()

# 833


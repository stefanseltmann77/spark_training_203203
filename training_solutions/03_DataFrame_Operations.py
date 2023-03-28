from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: How many rows of data are there?
df.count()

# %% Exercise 02: Look at the Schema. Extract the datatypes,
# that are or type integer.
# Make a list of the column names of integers
df.dtypes

cols_int = [col[0] for col in df.dtypes if col[1] == 'int']
# or also with tuple unpacking
cols_int = [name for name, type in df.dtypes if type == 'int']

# or not so elegant
cols_int = []
for name, type in df.dtypes:
    if type == 'int':
        cols_int.append(name)

# %% Exercise 03: select the columns isbn13 and original_publication_year.
# What types are they? Cast them to a more appropriate formats
df.select(sf.col("isbn13").cast("string"),
          sf.col("original_publication_year").cast("integer"))

#  or better with string objects
from pyspark.sql.types import  StringType, IntegerType
df.select(sf.col("isbn13").cast(StringType()),
          sf.col("original_publication_year").cast(IntegerType()))

# or
df.withColumn("isbn13", sf.col("isbn13").cast("string")) \
  .withColumn("original_publication_year", sf.col("original_publication_year").cast("integer"))

# %% Exercise 04: Drop the two columns that contain urls
df = df.drop(*[col for col in df.columns if col.endswith("url")])

# %% Exercise 05: Create new columns,
# - Set the current date in a column: work_dt
# - top_rating_flg, if average_rating above 2.0 or not
# - contemporary_flg, if original_publication_year greater than 2000
# commands: "case when" ->  when().otherwise()

import datetime
from pyspark.sql.functions import when, lit
df = df.withColumn("work_dt", sf.current_date())
df = df.\
    withColumn("top_rating_flg",
                   sf.when(df.average_rating > 2, 1).otherwise(0)).\
    withColumn("original_publication_year",
                   sf.when(df.average_rating > 2000, 1).otherwise(0))

# %% Exercise 06: Check if there are any missings
df.describe().show()
# or with more detail
df.describe().toPandas().iloc[0].transpose()

# or directly and dynamic
null_checks = [
    sf.avg(
        sf.when(sf.isnull(df[col]),1).
            otherwise(0)).alias(f"nulls_for_{col}")
    for col in df.columns]
df.agg(*null_checks).show(vertical=True)

# %% Exercise 07: How many rows are left, if we discard all missings?
df.dropna().count()

# %% Exercise 08: Which books have no language_code? What are the original titles.
df.where(sf.isnull(df.language_code)).show()


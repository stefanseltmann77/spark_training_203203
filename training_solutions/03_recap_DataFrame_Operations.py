from pyspark.sql import functions as sf
from pyspark.sql.types import StringType

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")
# %% Exercise 01: How many rows of data are there?
df.count()

# %% Exercise 02: What columns are there and which datatypes?
df.printSchema()
# or
df.dtypes

# %% Exercise 03: cast the zipcodeOri to string
from pyspark.sql.types import StringType
import pyspark.sql.functions as sf

df.withColumn('zipcodeOri', df.zipcodeOri.cast("String"))
# or better
df.withColumn('zipcodeOri', df.zipcodeOri.cast(StringType()))

# %% Exercise 04: Build a new flag (0/1) that signifies if the customer is female
from pyspark.sql import functions as sf

is_female = sf.when(sf.col("gender") == "F", 1).otherwise(0)
df = df.withColumn("is_female", is_female)
df.crosstab('gender', 'is_female').show()

# %% Exercise 05: How many distinct customers are there?
from pyspark.sql import functions as sf

# best version
df.agg(sf.countDistinct("customer")).show()
# or
df.select("customer").distinct().count()
# or
df.dropDuplicates(subset=["customer"]).count()
# or
df.selectExpr("count(distinct customer)").show()
# or, if it has to be
df.agg(sf.expr("count(distinct customer)")).show()

# solution = 4112
# %% Exercise 06: How many fraud cases are there?
df.groupby("fraud").count().show()
# or better
df.where(df.fraud == 1).count()
# or
df.where(sf.col("fraud") == 1).count()

# %% Exercise 07: How many distinct female fraud customers are there. Use the new flag for is_female?
df.where((df.fraud == 1) & (df.is_female == 1)). \
    select("customer").distinct().count()

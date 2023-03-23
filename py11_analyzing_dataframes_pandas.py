# %% SETUP ############################################################################################################
from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark
import pandas as pd
import matplotlib.pyplot as plt

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./data/recipeData.csv", inferSchema=True, header=True, encoding="utf8").cache()
df.printSchema()


# %% Switch to Pandas ##################################################################################################
pdf = df.toPandas()

pdf.StyleID.value_counts()

pdf.describe()
boil_times = pdf.groupby("StyleID").agg({"BoilTime": ['mean', 'max']})

boil_times.plot()
plt.show()


# %% And back to Spark: ################################################################################################
spark.createDataFrame(boil_times)
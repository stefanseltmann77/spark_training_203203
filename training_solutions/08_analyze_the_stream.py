from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 800)

#########################################################
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType

stream_path = "data/stream"
schema = StructType([StructField("BeerID", LongType(), False),
                     StructField("DrinkingTime", TimestampType(), False),
                     StructField("DrinkingVolume", DoubleType(), False),
                     StructField("Consumer", StringType(), False)]
                    )

stream_handle_static = spark.read.csv(schema=schema, path=stream_path, sep="\t")
stream_handle_static.describe().show(truncate=False)

df_beers = spark.read.csv("./data/recipeData.csv", inferSchema=True, header=True)

# normal dataframe operation, still no streaming
stream_handle_static.isStreaming
# everytime, we run it, it gets bigger


# %% Exercise 01: ######################################################################################################
# Create an STATIC aggregation that shows the amount of drank beer based on the beerID


stream_handle_static.groupBy("BeerID").agg(sf.sum("DrinkingVolume")).show()

# %% Exercise 02: ######################################################################################################
# Join the df_beers to your aggregation using the BeerID, and display the volumne based on the Beer Name as a new df

stream_handle_static. \
    groupBy("BeerID"). \
    agg(sf.round(sf.sum("DrinkingVolume"), 2).alias("vol")). \
    join(df_beers, "BeerID"). \
    select(df_beers['Name'], "vol"). \
    orderBy("vol", ascending=False). \
    show(truncate=False)

# %% Exercise 03: ######################################################################################################
# Stream the result to console
# define a new streamHandle that is not static with readStream
# apply the aggregation and join to it
# use writeStream with outputmode "complete"
# Optional A: Do it only every Minute
# Optional B: Do it only for the last 5 Minutes  (GroupBy Window)

stream_handle_stream = spark.readStream.csv(schema=schema, path=stream_path, sep="\t")
stream_handle_stream.isStreaming

from pyspark.sql.functions import window

stream = stream_handle_stream. \
    groupBy("BeerID", window("DrinkingTime", "5 minute")). \
    agg(sf.round(sf.sum("DrinkingVolume"), 2).alias("vol")). \
    join(sf.broadcast(df_beers.cache()), "BeerID"). \
    select(df_beers['Name'], "vol"). \
    orderBy("vol", ascending=False). \
    writeStream. \
    format("console"). \
    outputMode("complete"). \
    trigger(processingTime='1 minute'). \
    start()

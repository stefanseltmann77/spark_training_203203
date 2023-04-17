from pyspark.sql import functions as sf, DataFrame

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: How many distinct authors are there?
# Please use an aggregation and not the df.distinct() method.

df.agg(sf.countDistinct("authors").alias("distcount")).show()
# or also
df.groupby("authors").count().count()

# %% Exercise 02: which 10 authors have the most books in the sample
df. \
    groupby("authors"). \
    count(). \
    orderBy("count", ascending=False)
# or
df. \
    groupby("authors"). \
    agg(sf.count(df.authors).alias("book_cnt")). \
    orderBy("book_cnt", ascending=False)

# or in two steps
df_by_author: DataFrame = df.groupby("authors")
book_count: DataFrame = df_by_author.count()
book_count.orderBy("count", ascending=False).limit(10)

top_authors = book_count.orderBy("count", ascending=False).limit(10)

# %% Exercise 03: What is the mean, maximum and minimum average_rating
# for these authors and the earliest original_publication_year.
result = df_by_author.agg(sf.count(df.authors).alias("book_cnt"),
                          sf.avg(df.average_rating),
                          sf.max(df.average_rating),
                          sf.min(df.average_rating),
                          sf.min(df.original_publication_year)). \
    orderBy("book_cnt", ascending=False).limit(10)

# or dynamically and also elegant:
summary_funcs = {'avg': sf.mean, 'top': sf.max, 'bottom': sf.min}
summary_columns = ["average_rating", "original_publication_year"]

df_by_author. \
    agg(sf.count("average_rating").alias("counter"),
        *[func(col).alias(col + "_" + label)
          for label, func in summary_funcs.items()
          for col in summary_columns]). \
    orderBy("counter", ascending=False).limit(10).printSchema()

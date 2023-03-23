from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# df.where(df['authors'].like('%Iain%Banks%')).orderBy('original_publication_year').show()

# %% Exercise 01: How many distinct authors are there?
# Please use an aggregation and not the df.distinct() method.
df_by_author = df.groupby("authors")

df_by_author.count().count()  # 4664
df.select(sf.count_distinct('authors')).show()  # 4664
df.agg(sf.count_distinct('authors')).show()  # 4664

# %% Exercise 02: which 10 authors have the most books in the sample
df_by_author.count().orderBy('count', ascending=False).limit(10).show()

df = df.groupBy(df.authors).\
    agg(sf.sum(sf.lit(1)).alias("count"),
        sf.count("*")).\
    sort(sf.desc("count"))

# %% Exercise 03: What is the mean, maximum and minimum average_rating
# for these authors and the earliest original_publication_year.

# simple
df_by_author.agg(sf.count('*').alias('count'),
                 sf.mean('average_rating'),
                 sf.max('average_rating'),
                 sf.min('average_rating'),
                 sf.min('original_publication_year'),
                 ).orderBy('count', ascending=False).limit(10).show()

# fancy
agg_columns = [sf.mean('average_rating'),
               sf.max('average_rating'),
               sf.min('average_rating')]

df_by_author.agg(sf.count('*').alias('count'),
                 *agg_columns,
                 sf.min('original_publication_year'),
                 ).orderBy('count', ascending=False).limit(10).show()

agg_funcs = [sf.mean, sf.max, sf.min, sf.sum]
analysis_vars = ['average_rating', 'original_publication_year']
agg_columns = [agg_func(analysis_var)
               for agg_func in agg_funcs
               for analysis_var in analysis_vars]

df_by_author.agg(sf.count('*').alias('count'),
                 *agg_columns,
                 sf.min('original_publication_year'),
                 ).orderBy('count', ascending=False).limit(10).show()


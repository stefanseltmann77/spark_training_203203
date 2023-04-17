from pyspark.sql import functions as sf

from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: Read the ratings.csv, tags.csv, book_tags.csv  and get familiar with the columns.
default_reader = spark.read.format("csv").options(header=True, inferSchema=True)

df = default_reader.load("./training_data/books.csv")
df_ratings = default_reader.load("./training_data/ratings.csv")
df_tags = default_reader.load("./training_data/tags.csv")
df_book_tags = default_reader.load("./training_data/book_tags.csv")

# %% Exercise 02: Check all four dataframes for columns to join with
df_ratings.columns
df_tags.columns
df_book_tags.columns

# you can use sets
set(df_book_tags.columns).intersection(set(df_tags.columns))
set(df.columns).intersection(set(df_ratings.columns))

# %% Exercise 03: Take the book table and join it first with the ratings.
# Get the count!
df.join(df_ratings, "book_id").count()

# %% Exercise 04: Which User (by user_id) rated the most books
df_top_user = df_ratings. \
    groupby("user_id"). \
    count(). \
    orderBy("count", ascending=False). \
    limit(2)

# %% Exercise 05: What are the frequent tags for the books of this user?
# Display the labels
df_join = df_book_tags. \
    join(sf.broadcast(df), on=df.book_id == df_book_tags.goodreads_book_id). \
    join(sf.broadcast(df_tags), on="tag_id"). \
    join(df_ratings, on="book_id"). \
    join(sf.broadcast(df_top_user), on="user_id", how="leftsemi")
df_join.groupby("tag_name").count().orderBy("count", ascending=False).show()

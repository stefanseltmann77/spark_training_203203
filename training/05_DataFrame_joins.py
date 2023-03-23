from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./training_data/books.csv", header=True, inferSchema=True)

# %% Exercise 01: Read the ratings.csv, tags.csv, book_tags.csv and get familiar with the columns.
# What columns are there in each dataset?
reader = spark.read.format('csv').options(header=True, inferSchema=True)

df_books = reader.load("./training_data/books.csv")
df_ratings = reader.load("./training_data/ratings.csv")
df_tags = reader.load("./training_data/tags.csv")
df_book_tags = reader.load("./training_data/book_tags.csv")

# %% Exercise 02: Check all four DataFrames for columns to join with.

set(df_books.columns).intersection(df_ratings.columns)
set(df_tags.columns).intersection(df_book_tags.columns)
set(df_book_tags.columns).intersection(df_books.columns)  # ??

# %% Exercise 03: Take the book table and join it first with the ratings. What is the count?

df_ratings.cache().count()

df_join_book_ratings = df_books.cache().join(df_ratings, on='book_id')
df_join_book_ratings.cache().count()  # 79701

# %% Exercise 04: Which User (by user_id) rated the most books?
from pyspark.sql import functions as sf
df_top_user = df_join_book_ratings.groupby('user_id').\
    agg(sf.count('rating').alias('count')).\
    orderBy('count', ascending=False)

df_top_user.show()
# 11927, 23612

# %% Exercise 05: What are the frequent tags for the books of this user?
# Display the labels


result = df_tags.\
    join(df_book_tags, on='tag_id').\
    join(df_join_book_ratings, on=df_book_tags.goodreads_book_id == df_join_book_ratings.book_id).\
    join(df_top_user.limit(2), on=df_top_user.user_id == df_join_book_ratings.user_id, how='semi')

result.groupby('tag_name').count().orderBy('count', ascending=False).explain()

# don't do!
result = df_tags.\
    join(df_book_tags, on='tag_id').\
    join(df_books, on=df_book_tags['goodreads_book_id'] == df_books['book_id']).\
    join(df_ratings, on=df_ratings['book_id'] == df_books['book_id']).\
    where(df_ratings.user_id == df_top_user.limit(1).take(1)[0].user_id)

result.groupby('tag_name').count().orderBy('count', ascending=False).show()


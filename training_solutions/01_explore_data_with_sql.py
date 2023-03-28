from pyspark.sql import functions as sf
from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

# %% Exercise 01: Read the date from  "./training_data/books.csv"
# Use the SparkSession "spark" and read a csv as a DataFrame called "df".
# The file has a header. The schema can be inferred.
df = spark.read.csv("./training_data/books.csv", inferSchema=True, header=True)

# %% Exercise 02: Get the columns of the DataFrame and/or Print its schema.
df.columns
# or
df.dtypes
# or
df.printSchema()

# %% Exercise 03: Register the Dataframe as a table called "books"
# dataframe -> createOrReplaceTempView
df.createOrReplaceTempView("books")

# %% Exercise 04: How many books are in the dataset
# spark -> sql
spark.sql("SELECT count(*) FROM books").show()

# %% Exercise 05: Make a "limited" Select of just 100 rows
spark.sql("SELECT * FROM books").limit(100).show()
spark.sql("SELECT * FROM books").show(100)
spark.sql("SELECT * FROM books LIMIT 100").show(100)

# %% Exercise 06: Which languages are there (column language_code)
# and which are the most frequent ones?
spark.sql("""
    SELECT language_code, 
        count(*)
     FROM books 
     GROUP BY 1 ORDER BY 2 desc""").show()

# %% Exercise 07: What are the titles of the german books?
spark.sql("SELECT title FROM books WHERE language_code = 'ger'").show()

# %% Exercise 08: Make a summary of ratings (column average_rating) by publication year (max, min, mean)
df_summary = spark.sql("SELECT "
          " max(average_rating) , "
          " avg(average_rating) , "
          " min(average_rating) ,  "
          " cast(original_publication_year as int) as year_int"
          " FROM books "
          "group by original_publication_year")

df_summary.columns

# %% Exercise 09: Save the result of the summary to a new csv file, with header
# commands: [some_dataframe].write.csv
df_summary.write.csv("my_summary", header=True)



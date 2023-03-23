from spark_setup_spark3 import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

# %% Exercise 01: ######################################################################################################
# Create several separate DataFrames from the data using selects:
# One dimension for the Customers containing customer, age, gender and zipcodeOri, unique of course.
# One for the Merchants containing merchant, zipMerchant, also unique.
# One fact-table for the Transactions with category, amount and fraud, ... and the columns to join both the merchant
# and the customer back in again.
df_customer = df.select('customer', 'age', 'gender', 'zipcodeOri').distinct()
df_merchant = df.select('merchant', 'zipMerchant').distinct()
df_facts = df.select('step', 'category', 'amount', 'fraud', 'customer', 'merchant').distinct()

# %% Exercise 02: ######################################################################################################
# How many customers are there? How many merchants and how many transactions?
# Use your new DataFrames
print(df_customer.count(), df_merchant.count(), df_facts.count())
# 4112 50 594643

# %% Exercise 03: #############################################################
# Which category of transaction causes the biggest total loss in frauds?
# What amount is lost?
# Use your new table for transactions
import pyspark.sql.functions as sf
df_facts.freqItems(['fraud']).show()

df_frauds = df_facts.where(df_facts.fraud == 1)

df_frauds.\
    groupby('category').\
    agg(sf.sum('amount').alias('loss')).\
    orderBy('loss', ascending=False).limit(1).show()

+---------+------------------+
| category|              loss|
+---------+------------------+
|es_travel|1537944.0600000012|
+---------+------------------+
# %% Exercise 04: ######################################################################################################
# Which category has the highest single fraud case by amount?
# Use your new table for transactions

df_frauds.\
    orderBy('amount', ascending=False).\
    select('category', 'amount').limit(1).show()
+---------+-------+
| category| amount|
+---------+-------+
|es_travel|8329.96|
+---------+-------+


# %% Exercise 05: ######################################################################################################
# How old is the customer that has the most fraud cases in travel.
# Use your new DataFrames instead of the imported table via a join.

df_frauds.\
    where(df_facts.category.like('%travel%')). \
    join(df_customer, on='customer').\
    groupby('customer', 'age').\
    agg(sf.count('*').alias('count')).orderBy('count', ascending=False).\
    select('age', 'customer').limit(1).show()

+---+----------+
|age|  customer|
+---+----------+
|  2|C806399525|
+---+----------+





# %% Exercise 06: ###############################################################
# Try calculating a share from the total loss per category that every
# fraud is causing.
# For example x% from all losses related to es_travel, and so on.
# use windowing functions. see pyspark.sql Window
from pyspark.sql import Window
win_spec = Window.partitionBy('category')

col_category_loss = sf.sum('amount').over(win_spec).alias('category_loss')

df_frauds.\
    select('category', 'amount',
           col_category_loss,
           (sf.round(df_facts.amount / col_category_loss,2)).alias('category_loss_share')).show()

df_frauds.\
    select('category', 'amount',
           sf.sum('amount').over(win_spec).alias('category_loss')).\
    withColumn('category_loss_share', sf.col('amount')/sf.col('category_loss')).show()




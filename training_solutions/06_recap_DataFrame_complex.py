from pyspark.sql import functions as sf
from spark_setup_spark3p import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("./data/bs140513_032310.csv", header=True, inferSchema=True, quote="'")

df.show()

# %% Exercise 01: ######################################################################################################
# Create several separate DataFrames from the data using selects:
# One dimension for the Customers containing customer, age, gender and zipcodeOri, unique of course.
# One for the Merchants containing merchant, zipMerchant, also unique.
# One fact-table for the Transactions with category, amount and fraud, ... and the columns to join both the merchant
# and the customer back in again.
df_customer = df.select('customer',
                        'age',
                        'gender',
                        'zipcodeOri').distinct()
df_merchants = df.select('merchant',
                         'zipMerchant').distinct()
df_transactions = df.select(
    'customer',
    'merchant',
    'category',
    'amount',
    'fraud')

# %% Exercise 02: ######################################################################################################
# How many customers are there? How many merchants and how many transactions?
# Use your new DataFrames
for df in (df_customer, df_merchants, df_transactions):
    print(df.count())
4112, 50, 594643

# %% Exercise 03: #############################################################
# Which category of transaction causes the biggest total loss in frauds?
# What amount is lost?
# Use your new table for transactions
df_transactions.columns

df_transactions. \
    where(df_transactions.fraud == 1). \
    groupBy("category"). \
    agg(sf.sum("amount").alias("amount")). \
    orderBy("amount", ascending=False).show()

# es_travel | 1537944.0600000003

# or also
df_transactions \
    .where(df_transactions.fraud == 1) \
    .select(df_transactions.category, df.amount) \
    .groupBy(df_transactions.category) \
    .sum().orderBy("sum(amount)", ascending=False) \
    .show()

# %% Exercise 04: ######################################################################################################
# Which category has the highest single fraud case by amount?
# Use your new table for transactions
df_transactions. \
    where(df_transactions.fraud == 1). \
    orderBy("amount", ascending=False). \
    select("category").limit(1).show()
# +---------+
# | category |
# +---------+
# | es_travel |
# +---------+

# %% Exercise 05: ######################################################################################################
# How old is the customer that has the most fraud cases in travel.
# Use your new DataFrames instead of the imported table via a join.


df_transactions. \
    where(df_transactions.fraud == 1). \
    where(sf.lower(df_transactions.category).like("%travel%")). \
    join(df_customer, on="customer"). \
    groupby("customer", "age").count(). \
    orderBy("count", ascending=False).limit(1).show()

# %% Exercise 06: ###############################################################
# Try calculating a share from the total loss per category that every
# fraud is causing.
# For example x% from all losses related to es_travel, and so on.
# use windowing functions. see pyspark.sql Window
from pyspark.sql import Window

window_spec = Window().partitionBy(sf.col("category"))

df_transactions.where(df_transactions.fraud == 1). \
    select(df_transactions.category,
           df_transactions.amount,
           sf.sum(df_transactions.amount).over(window_spec).
           alias("amount_per_category"),
           (df_transactions.amount /
            sf.sum(df_transactions.amount).over(window_spec)).
           alias("amount_share_pct")
           ).show()

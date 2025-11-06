from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("OrderPipeline").getOrCreate()

order_items = spark.read.option('header', 'true').csv('order_items.csv')
order_item_options = spark.read.option('header', 'true').csv('order_item_options.csv')
date_dim = spark.read.option('header', 'true').csv('date_dim.csv')

print("order_items count:", order_items.count())
print("order_item_options count:", order_item_options.count())
print("date_dim count:", date_dim.count())

order_items.printSchema()
order_item_options.printSchema()
date_dim.printSchema()

def inspect_nulls(df, name):
    print(f"Checking nulls in {name}:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"Column {column} has {null_count} null values.")
        else:
            print(f"Column {column} has no null values.")

inspect_nulls(order_items, 'order_items')
inspect_nulls(order_item_options, 'order_item_options')
inspect_nulls(date_dim, 'date_dim')
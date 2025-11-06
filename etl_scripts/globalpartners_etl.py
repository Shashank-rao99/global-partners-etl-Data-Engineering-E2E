import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.sparkContext.setLogLevel("DEBUG")

jdbc_url = "jdbc:sqlserver://database-1.c07yme0eefsh.us-east-1.rds.amazonaws.com:1433;databaseName=GlobalPartners"
jdbc_properties = {
    "user": "admin",
    "password": "xxxxxx",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Process order_items
table_name = "dbo.order_items"
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
df.show(10)
print(f"Number of rows fetched: {df.count()}")
s3_path = "s3://global-partners-project/order_items/"
df.write.mode("overwrite").parquet(s3_path)
print(f"Data written to S3 at {s3_path}")

# Process order_item_options
table_name = "dbo.order_item_options"
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
df.show(10)
print(f"Number of rows fetched: {df.count()}")
s3_path = "s3://global-partners-project/order_item_options/"
df.write.mode("overwrite").parquet(s3_path)
print(f"Data written to S3 at {s3_path}")

# Process date_dim
table_name = "dbo.date_dim"
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
df.show(10)
print(f"Number of rows fetched: {df.count()}")
s3_path = "s3://global-partners-project/date_dim/"
df.write.mode("overwrite").parquet(s3_path)
print(f"Data written to S3 at {s3_path}")

job.commit()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, rank, udf, col, broadcast, coalesce, avg, when, lit, month

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataAnalysis") \
    .getOrCreate()

# Define Schemas
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price_per_unit", DoubleType(), True),
    StructField("discount", DoubleType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_signup_date", TimestampType(), True)
])

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_supplier_id", StringType(), True)
])

# Load Data
sales_df = spark.read.csv("sales_data.csv", header=True, schema=sales_schema)
customer_df = spark.read.csv("customer_data.csv", header=True, schema=customer_schema)
product_df = spark.read.csv("product_data.csv", header=True, schema=product_schema)

# Repartition and Bucketing for Data Skew
sales_df = sales_df.repartition("product_id")
customer_df = customer_df.repartition("customer_city")

product_df.write.bucketBy(8, "product_id").saveAsTable("product_data_bucketed")
sales_df.write.bucketBy(8, "customer_id").saveAsTable("sales_data_bucketed")

# Broadcast Join for Optimized Join Performance
joined_df = sales_df.join(broadcast(customer_df), "customer_id", "inner")

# Join with Product Data
full_df = joined_df.join(product_df, "product_id", "inner")

# Window Function: Running Total Sales by Product per Month
window_spec = Window.partitionBy("product_id", month("transaction_date")).orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
full_df = full_df.withColumn("running_total_sales", sum(col("quantity") * col("price_per_unit")).over(window_spec))

# Window Function: Ranking Customers within each City by Spending
window_spec_rank = Window.partitionBy("customer_city").orderBy(col("total_spending").desc())
customer_spending = full_df.groupBy("customer_id", "customer_city").agg(sum(col("quantity") * col("price_per_unit")).alias("total_spending"))
customer_spending = customer_spending.withColumn("rank", rank().over(window_spec_rank))

# Accumulator for Tracking Skewed Data
skewed_acc = spark.sparkContext.accumulator(0)

def detect_skewed_data(quantity):
    if quantity > 50:
        skewed_acc.add(1)
    return quantity

detect_skewed_udf = udf(detect_skewed_data, IntegerType())
full_df = full_df.withColumn("quantity_checked", detect_skewed_udf(col("quantity")))

# UDF for Categorizing Transactions as High or Low Value
def categorize_transaction(total_amount):
    if total_amount > 1000:
        return "High Value"
    else:
        return "Low Value"

categorize_udf = udf(categorize_transaction, StringType())
full_df = full_df.withColumn("transaction_category", categorize_udf(col("quantity") * col("price_per_unit") - col("discount")))

# Coalesce to Reduce Partitions for Small Datasets
coalesced_customer_df = customer_df.coalesce(2)

# Final Aggregations
top_products = full_df.groupBy("product_category", "product_name") \
                      .agg(sum(col("quantity") * col("price_per_unit")).alias("total_revenue")) \
                      .orderBy(col("total_revenue").desc()) \
                      .limit(5)

top_cities_cltv = full_df.groupBy("customer_city").agg(avg(col("quantity") * col("price_per_unit") - col("discount")).alias("avg_cltv")) \
                         .orderBy(col("avg_cltv").desc()) \
                         .limit(3)

# Cache for Repeated Computations
full_df.cache()

# Display Results
top_products.show()
top_cities_cltv.show()

# Stop Spark session
spark.stop()

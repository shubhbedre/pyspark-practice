from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, udf, lit, avg, row_number, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType
from pyspark.sql.window import Window

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Telecom Analytics and Fraud Detection") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()

# Step 2: Define explicit schema for customer data
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("plan_type", StringType(), True),
    StructField("monthly_fee", DoubleType(), True),
    StructField("missing_value", DoubleType(), True)
])

# Load customer data with the defined schema
customer_df = spark.read.schema(customer_schema).json("customer_data.json")

# Step 3: Define schema for usage data
usage_schema = StructType([
    StructField("usage_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("call_duration", DoubleType(), True),
    StructField("sms_count", IntegerType(), True),
    StructField("data_usage", DoubleType(), True),
    StructField("usage_date", TimestampType(), True),
    StructField("usage_type", StringType(), True)
])

# Load usage data
usage_df = spark.read.schema(usage_schema).csv("usage_data.csv", header=True)

# Step 4: Load billing data from Parquet
billing_df = spark.read.parquet("billing_data.parquet")

# Step 5: Data Cleaning - Handle missing values in customer data
customer_df = customer_df.fillna({"missing_value": 0})

# Step 6: Feature Engineering - Calculate total call, SMS, and data usage per customer
usage_agg_df = usage_df.groupBy("customer_id").agg(
    _sum(when(col("usage_type") == "Call", col("call_duration")).otherwise(0)).alias("total_call_duration"),
    _sum(when(col("usage_type") == "SMS", col("sms_count")).otherwise(0)).alias("total_sms_count"),
    _sum(when(col("usage_type") == "Data", col("data_usage")).otherwise(0)).alias("total_data_usage")
)

# Step 7: Join customer data with usage data for complete customer usage analytics
customer_usage_df = customer_df.join(usage_agg_df, on="customer_id", how="left")

# Step 8: Advanced Analytics with Spark SQL - Calculate monthly bills and identify high usage
billing_df.createOrReplaceTempView("billing")
spark.sql("""
    SELECT customer_id, billing_month, SUM(total_amount) as total_bill
    FROM billing
    GROUP BY customer_id, billing_month
    HAVING SUM(total_amount) > 100
""").show()

# Step 9: Fraud Detection - Flag customers with suspicious billing and usage patterns
# Define UDF to detect fraud based on simple rule: High usage and high billing
def detect_fraud(total_bill, total_usage):
    return 1 if total_bill > 200 and total_usage > 500 else 0

fraud_udf = udf(detect_fraud, IntegerType())

fraud_df = customer_usage_df.join(billing_df, on="customer_id").withColumn(
    "fraud_detected", fraud_udf(col("total_bill"), col("total_data_usage"))
)

# Step 10: Optimization - Repartitioning and Caching
fraud_df = fraud_df.repartition("customer_id").cache()

# Step 11: Save final results
fraud_df.write.format("delta").save("output/fraud_detection")

# Stop Spark session
spark.stop()

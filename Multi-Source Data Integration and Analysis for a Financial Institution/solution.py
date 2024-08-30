from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
import xml.etree.ElementTree as ET

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Financial Institution Data Integration") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()

# Define schema for transactions
transactions_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("category", StringType(), True),
    StructField("status", StringType(), True)
])

# Load transactions data
transactions_df = spark.read.schema(transactions_schema).json("transactions.json")

# Define schema for credit scores
credit_scores_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("credit_utilization", DoubleType(), True),
    StructField("credit_limit", IntegerType(), True),
    StructField("missing_value", DoubleType(), True)
])

# Load credit scores data
credit_scores_df = spark.read.schema(credit_scores_schema).csv("credit_scores.csv", header=True)

# Define UDF to parse XML feedback
def parse_feedback(xml_str):
    root = ET.fromstring(xml_str)
    return root.find("Text").text if root is not None else None

parse_feedback_udf = udf(parse_feedback, StringType())

# Load and parse customer feedback data
feedback_rdd = spark.sparkContext.wholeTextFiles("customer_feedback.xml").map(lambda x: x[1])
feedback_df = feedback_rdd.map(lambda x: parse_feedback(x)).toDF(["feedback_text"])

# Handle missing values
credit_scores_df = credit_scores_df.withColumn(
    "credit_utilization",
    when(col("credit_utilization").isNull(), mean("credit_utilization").over(Window.orderBy())).otherwise(col("credit_utilization"))
)

# Perform inner join on customer_id
joined_df = transactions_df.join(credit_scores_df, on="customer_id", how="inner")

# Aggregation with window function: calculate rolling average of transaction amounts
window_spec = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(-2, 0)
joined_df = joined_df.withColumn("rolling_avg_amount", mean("amount").over(window_spec))

# Broadcast join to optimize query
broadcasted_df = joined_df.join(spark.broadcast(feedback_df), joined_df.customer_id == feedback_df.customer_id, "left")

# Handle data skew by repartitioning
repartitioned_df = broadcasted_df.repartition(100, col("customer_id"))

# Bucketing for efficient querying
bucketed_df = repartitioned_df.write.bucketBy(50, "customer_id").saveAsTable("bucketed_customer_data")

# Save final data as Delta table
bucketed_df.write.format("delta").save("/delta/customer_data")

# Close the Spark session
spark.stop()

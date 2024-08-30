from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, udf, lit, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Fraud Detection System") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .getOrCreate()

# Define schema for historical transactions
historical_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("is_fraud", IntegerType(), True)
])

# Load historical transactions
historical_df = spark.read.schema(historical_schema).csv("historical_transactions.csv", header=True)

# Define schema for customer profiles
customer_profiles_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("missing_value", DoubleType(), True)
])

# Load customer profiles
customer_profiles_df = spark.read.schema(customer_profiles_schema).json("customer_profiles.json")

# Load real-time streaming transactions
streaming_df = spark.read.parquet("streaming_transactions.parquet")

# Handle missing values in customer profiles
customer_profiles_df = customer_profiles_df.withColumn(
    "account_balance",
    when(col("account_balance").isNull(), mean("account_balance").over(Window.orderBy())).otherwise(col("account_balance"))
)

# Feature Engineering: Create a transaction frequency feature
window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")
historical_df = historical_df.withColumn("transaction_count", lit(1)) \
                             .withColumn("transaction_frequency", _sum("transaction_count").over(window_spec))

# Normalize transaction amounts (assume all amounts are in USD for simplicity)
currency_conversion = {"USD": 1, "EUR": 1.1, "INR": 0.013, "GBP": 1.3, "JPY": 0.009}
currency_udf = udf(lambda currency: currency_conversion.get(currency, 1.0), DoubleType())
historical_df = historical_df.withColumn("normalized_amount", col("transaction_amount") * currency_udf(col("currency")))

# Join historical transactions with customer profiles
joined_df = historical_df.join(customer_profiles_df, on="customer_id", how="inner")

# Real-time fraud detection: Identify suspicious transactions using a basic rule-based approach
def is_suspicious(transaction_amount, account_balance, transaction_frequency):
    return 1 if transaction_amount > account_balance * 0.5 and transaction_frequency > 5 else 0

suspicious_udf = udf(is_suspicious, IntegerType())

streaming_df = streaming_df.withColumn(
    "is_suspicious",
    suspicious_udf(col("transaction_amount"), col("account_balance"), col("transaction_frequency"))
)

# Broadcast join for optimization
broadcasted_profiles = spark.broadcast(customer_profiles_df)
optimized_join = streaming_df.join(broadcasted_profiles, on="customer_id", how="inner")

# Handling Data Skew: Repartitioning based on customer_id
repartitioned_df = optimized_join.repartition("customer_id")

# Saving the final suspicious transactions to a Delta table
repartitioned_df.write.format("delta").save("suspicious_transactions")

# Stop the Spark session
spark.stop()

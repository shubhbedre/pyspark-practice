from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, rank, col, row_number, month

# Initialize Spark session
spark = SparkSession.builder.appName("SalesDataAnalysis").getOrCreate()

# Load the data
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# 1. Rank customers based on their total spending
customer_spending = df.groupBy("customer_id").agg(sum("amount").alias("total_spending"))
window_spec = Window.orderBy(col("total_spending").desc())
customer_spending_ranked = customer_spending.withColumn("rank", rank().over(window_spec))
customer_spending_ranked.show()

# 2. Find the top 5 customers by spending for each month
df = df.withColumn("month", month("transaction_date"))
monthly_spending = df.groupBy("customer_id", "month").agg(sum("amount").alias("monthly_spending"))
window_spec_monthly = Window.partitionBy("month").orderBy(col("monthly_spending").desc())
top_5_customers_each_month = monthly_spending.withColumn("rank", rank().over(window_spec_monthly)) \
                                             .filter(col("rank") <= 5)
top_5_customers_each_month.show()

# 3. Calculate the running total of sales for each customer, ordered by the transaction date
window_spec_running_total = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_running_total = df.withColumn("running_total", sum("amount").over(window_spec_running_total))
df_running_total.show()

# 4. Identify the transaction with the highest amount for each customer and show the rank
window_spec_transaction = Window.partitionBy("customer_id").orderBy(col("amount").desc())
df_with_rank = df.withColumn("rank", row_number().over(window_spec_transaction))
highest_transaction_per_customer = df_with_rank.filter(col("rank") == 1).select("customer_id", "transaction_id", "amount", "rank")
highest_transaction_per_customer.show()

# Stop Spark session
spark.stop()

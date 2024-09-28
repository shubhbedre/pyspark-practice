from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, when, desc, to_date
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("ECommerceAnalytics").getOrCreate()

# Load the datasets
customers = spark.read.csv("customers.csv", header=True, inferSchema=True)
orders = spark.read.csv("orders.csv", header=True, inferSchema=True)
products = spark.read.csv("products.csv", header=True, inferSchema=True)

# Transformation 1: Handling missing values
customers = customers.na.fill({"city": "Unknown"})
orders = orders.na.fill({"order_amount": 0})

# Transformation 2: Join orders with products to get product details for each order
orders_with_products = orders.join(products, on="product_id", how="inner")

# Transformation 3: Join orders with customers to get customer details for each order
full_data = orders_with_products.join(customers, on="customer_id", how="inner")

# Action 1: Display the data
full_data.show(5)

# Transformation 4: Calculate total spending by each customer
total_spending = full_data.groupBy("customer_id", "customer_name").agg(sum("order_amount").alias("total_spent"))

# Action 2: Collect the top 5 spending customers
top_customers = total_spending.orderBy(desc("total_spent")).limit(5)
top_customers.show()

# Transformation 5: Calculate total sales per product category
category_sales = full_data.groupBy("category").agg(sum("order_amount").alias("total_sales"))

# Action 3: Show the sales per category
category_sales.show()

# Transformation 6: Count orders per city
orders_per_city = full_data.groupBy("city").agg(count("order_id").alias("total_orders"))

# Action 4: Show the number of orders per city
orders_per_city.show()

# Transformation 7: Detect missing or inconsistent data (e.g., negative prices)
invalid_data = full_data.filter(col("price") < 0)
invalid_data.show()

# Transformation 8: Add a column that flags high-value orders
full_data = full_data.withColumn("high_value_order", when(col("order_amount") > 500, 1).otherwise(0))

# Action 5: Count high-value orders
full_data.groupBy("high_value_order").agg(count("order_id")).show()

# Transformation 9: Window function - Calculate the rank of each order for each customer based on order amount
window_spec = Window.partitionBy("customer_id").orderBy(desc("order_amount"))
full_data = full_data.withColumn("order_rank", rank().over(window_spec))

# Action 6: Display top orders for each customer
full_data.filter(col("order_rank") == 1).show()

# Transformation 10: Aggregate sales by date
full_data = full_data.withColumn("order_date", to_date(col("order_date")))
sales_per_day = full_data.groupBy("order_date").agg(sum("order_amount").alias("daily_sales"))

# Action 7: Show the sales per day
sales_per_day.show()

# Transformation 11: Optimize by repartitioning data to handle data skew
repartitioned_data = full_data.repartition("customer_id")

# Caching the dataset for further operations
repartitioned_data.cache()

# Transformation 12: Coalesce after transformations to reduce the number of partitions
final_data = repartitioned_data.coalesce(4)

# Save final results
final_data.write.mode("overwrite").parquet("output/ecommerce_analytics")

# Stop Spark session
spark.stop()

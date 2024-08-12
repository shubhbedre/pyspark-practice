from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Initialize Spark session
spark = SparkSession.builder.appName("BasicEmployeeData").getOrCreate()

# Load the data
df = spark.read.csv("basic_employee_data.csv", header=True, inferSchema=True)

# 1. Show schema and the first 10 rows
df.printSchema()
df.show(10)

# 2. Filter data to include only employees aged above 40
filtered_df = df.filter(df['age'] > 40)
filtered_df.show()

# 3. Find the average salary of employees aged above 40
average_salary = filtered_df.agg(avg("salary")).first()[0]
print(f"Average salary of employees aged above 40: {average_salary}")

# 4. Group by age and count the number of employees in each age group
grouped_df = df.groupBy("age").count()
grouped_df.show()

# Stop Spark session
spark.stop()

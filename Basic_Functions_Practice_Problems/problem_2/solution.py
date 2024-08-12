from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeDataWithNulls").getOrCreate()

# Load the data
df = spark.read.csv("employee_data_with_nulls.csv", header=True, inferSchema=True)

# 1. Show count of null values for each column
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# 2. Drop rows where age or salary has null values
df_cleaned = df.dropna(subset=["age", "salary"])
df_cleaned.show()

# 3. Fill null values in the bonus column with the mean of non-null values
mean_bonus = df_cleaned.agg(avg("bonus")).first()[0]
df_filled = df_cleaned.fillna({"bonus": mean_bonus})
df_filled.show()

# 4. Calculate the total salary (salary + bonus) for each employee and add as a new column
df_with_total_salary = df_filled.withColumn("total_salary", col("salary") + col("bonus"))
df_with_total_salary.show()

# 5. Find the average total salary by department
average_total_salary_by_department = df_with_total_salary.groupBy("department").agg(avg("total_salary"))
average_total_salary_by_department.show()

# Stop Spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, col, rank
from pyspark.sql import Window

# Initialize Spark session
spark = SparkSession.builder.appName("ComplexEmployeeData").getOrCreate()

# Load the data
employee_df = spark.read.csv("complex_employee_data.csv", header=True, inferSchema=True)
department_df = spark.read.csv("department_data.csv", header=True, inferSchema=True)

# 1. Join employee and department DataFrames on department_id
joined_df = employee_df.join(department_df, on="department_id", how="inner")
joined_df.show()

# 2. Find the total salary paid by each department
total_salary_by_department = joined_df.groupBy("department_name").agg(sum(col("salary")).alias("total_salary"))
total_salary_by_department.show()

# 3. Identify departments where the total salary exceeds 5 million
high_salary_departments = total_salary_by_department.filter(col("total_salary") > 5000000)
high_salary_departments.show()

# 4. Find the department with the highest average salary
average_salary_by_department = joined_df.groupBy("department_name").agg(avg("salary").alias("average_salary"))
department_with_highest_avg_salary = average_salary_by_department.orderBy(col("average_salary").desc()).first()
print(f"Department with highest average salary: {department_with_highest_avg_salary['department_name']}")

# 5. For each department, find the employee with the highest salary
window_spec = Window.partitionBy("department_name").orderBy(col("salary").desc())
highest_salary_per_department = joined_df.withColumn("rank", rank().over(window_spec)) \
                                         .filter(col("rank") == 1) \
                                         .select("department_name", "name", "salary")
highest_salary_per_department.show()

# Stop Spark session
spark.stop()

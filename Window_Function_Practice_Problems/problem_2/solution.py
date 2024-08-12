from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, sum, row_number, round

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSalaryAnalysis").getOrCreate()

# Load the data
df = spark.read.csv("employee_salary_data.csv", header=True, inferSchema=True)

# 1. For each department, find the top 3 highest-paid employees
window_spec_top_3 = Window.partitionBy("department").orderBy(col("salary").desc())
top_3_employees = df.withColumn("rank", row_number().over(window_spec_top_3)) \
                    .filter(col("rank") <= 3)
top_3_employees.show()

# 2. Calculate the difference between each employee's salary and the average salary of their department
window_spec_avg_salary = Window.partitionBy("department")
df_with_avg_salary = df.withColumn("avg_salary", avg("salary").over(window_spec_avg_salary))
df_with_salary_diff = df_with_avg_salary.withColumn("salary_diff", col("salary") - col("avg_salary"))
df_with_salary_diff.show()

# 3. For each department, calculate the cumulative sum of salaries, ordered by the join date
window_spec_cumulative_sum = Window.partitionBy("department").orderBy("join_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_with_cumulative_sum = df.withColumn("cumulative_salary", sum("salary").over(window_spec_cumulative_sum))
df_with_cumulative_sum.show()

# 4. Find the percentage of total salary contributed by each employee within their department
window_spec_total_salary = Window.partitionBy("department")
df_with_total_salary = df.withColumn("total_salary", sum("salary").over(window_spec_total_salary))
df_with_percentage_contribution = df_with_total_salary.withColumn("percentage_contribution", round((col("salary") / col("total_salary")) * 100, 2))
df_with_percentage_contribution.select("emp_id", "department", "salary", "percentage_contribution").show()

# Stop Spark session
spark.stop()

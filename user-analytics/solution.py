from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from datetime import timedelta


# Initialize Spark session
spark = SparkSession.builder.appName("ComplexPySparkProblem").getOrCreate()

# Load the logs dataset
logs_df = spark.read.csv("/mnt/data/system_logs", header=False, inferSchema=True)
logs_df = logs_df.selectExpr("_c0 as user_id", "_c1 as action", "_c2 as timestamp")

# Parse timestamp and sort the logs
logs_df = logs_df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

# 1. Sessionization (User Action Sessionization)
window_spec = Window.partitionBy("user_id").orderBy("timestamp")
logs_df = logs_df.withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))

# Calculate time difference between consecutive actions
logs_df = logs_df.withColumn(
    "time_diff",
    F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
)

# Identify sessions where the time difference exceeds 30 minutes (1800 seconds)
logs_df = logs_df.withColumn(
    "new_session",
    F.when(F.col("time_diff") > 1800, 1).otherwise(0)
)

# Use cumulative sum to assign a session ID for each user's logs
logs_df = logs_df.withColumn(
    "session_id",
    F.sum("new_session").over(window_spec)
)

# 2. Top Active Users (Top 10 users with the most actions)
top_users_df = logs_df.groupBy("user_id").count()\
    .orderBy(F.desc("count"))\
    .limit(10)

# 3. Session Activity Summary
session_summary_df = logs_df.groupBy("user_id", "session_id")\
    .agg(
        F.count("action").alias("total_actions"),
        (F.max("timestamp").cast("long") - F.min("timestamp").cast("long")).alias("session_duration_seconds")
    )

# 4. Time Distribution Analysis (Hourly activity aggregation)
logs_df = logs_df.withColumn("hour_of_day", F.hour("timestamp"))
hourly_activity_df = logs_df.groupBy("user_id", "hour_of_day").agg(F.count("action").alias("actions_per_hour"))

# 5. Handle Data Skew (Repartition based on user_id)
skew_handling_df = logs_df.repartition(200, "user_id")

# 6. Identify Bot-Like Behavior (More than 100 actions in 5 minutes)
def detect_bot_behavior(actions, timestamps):
    for i in range(len(timestamps)):
        start_time = timestamps[i]
        end_time = start_time + timedelta(minutes=5)
        if sum(1 for t in timestamps if start_time <= t <= end_time) > 100:
            return True
    return False

# Register UDF to detect bot-like behavior
bot_udf = F.udf(detect_bot_behavior, BooleanType())

# Apply the UDF
bot_behavior_df = session_summary_df.withColumn(
    "bot_flag",
    bot_udf(F.collect_list("action").over(window_spec), F.collect_list("timestamp").over(window_spec))
)

# Final summary output
logs_df.show()
top_users_df.show()
session_summary_df.show()
hourly_activity_df.show()
bot_behavior_df.show()

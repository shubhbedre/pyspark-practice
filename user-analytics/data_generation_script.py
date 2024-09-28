import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random logs data
def generate_logs(num_users, num_logs):
    base_time = datetime.now()
    logs = []
    
    for _ in range(num_logs):
        user_id = f"user_{random.randint(1, num_users)}"
        action = random.choice(["click", "scroll", "purchase", "view"])
        # Generate a random timestamp within the last 24 hours
        timestamp = base_time - timedelta(minutes=random.randint(0, 1440))
        # Add some extra messy data
        extra_col = f"{action}_random_{random.randint(1, 10000)}"
        
        logs.append([user_id, action, timestamp, extra_col])
    
    return logs

# Generate the data
num_users = 1000  # Number of unique users
num_logs = 500000  # Total number of logs
logs_data = generate_logs(num_users, num_logs)

# Create a pandas DataFrame
df = pd.DataFrame(logs_data, columns=["user_id", "action", "timestamp", "extra_col"])

# Save the generated data to a CSV file
df.to_csv("user-analytics\data\system_logs.csv", index=False)

# Preview the generated data
print(df.head())

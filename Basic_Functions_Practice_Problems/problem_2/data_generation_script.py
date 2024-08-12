import random
import pandas as pd
import numpy as np

# Generate sample data with some null values
data = {
    "id": [i for i in range(1, 201)],
    "department": [random.choice(["HR", "Finance", "Engineering", "Marketing"]) for _ in range(200)],
    "age": [random.randint(20, 60) if random.random() > 0.1 else None for _ in range(200)],
    "salary": [random.randint(30000, 100000) if random.random() > 0.1 else None for _ in range(200)],
    "bonus": [random.randint(5000, 20000) if random.random() > 0.1 else None for _ in range(200)]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\problem_2\data\employee_data_with_nulls.csv", index=False)

import random
import pandas as pd

# Generate sample data
data = {
    "id": [i for i in range(1, 101)],
    "name": [f"Name_{i}" for i in range(1, 101)],
    "age": [random.randint(20, 60) for _ in range(100)],
    "salary": [random.randint(30000, 100000) for _ in range(100)]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv(r"data/basic_employee_data.csv", index=False)

import random
import pandas as pd

# Generate sales data
data = {
    "transaction_id": [i for i in range(1, 1001)],
    "customer_id": [random.randint(1, 200) for _ in range(1000)],
    "product_id": [random.randint(1, 50) for _ in range(1000)],
    "transaction_date": pd.date_range(start="2023-01-01", periods=1000, freq="H").tolist(),
    "amount": [random.uniform(10.0, 1000.0) for _ in range(1000)]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Window_Function_Practice_Problems\problem_1\data\sales.csv", index=False)

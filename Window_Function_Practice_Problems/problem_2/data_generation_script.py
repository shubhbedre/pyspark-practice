import random
import pandas as pd

# Generate employee salary data
data = {
    "emp_id": [i for i in range(1, 501)],
    "department": [random.choice(["HR", "Finance", "Engineering", "Marketing", "Sales"]) for _ in range(500)],
    "salary": [random.randint(40000, 150000) for _ in range(500)],
    "join_date": pd.date_range(start="2015-01-01", periods=500, freq="W").tolist()
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Window_Function_Practice_Problems\problem_2\data\employee_salary_data.csv", index=False)

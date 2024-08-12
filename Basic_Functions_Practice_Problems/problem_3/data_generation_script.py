import random
import pandas as pd

# Generate employee data
employee_data = {
    "emp_id": [i for i in range(1, 301)],
    "name": [f"Employee_{i}" for i in range(1, 301)],
    "department_id": [random.randint(1, 10) for _ in range(300)],
    "salary": [random.randint(40000, 120000) for _ in range(300)]
}

# Generate department data
department_data = {
    "department_id": [i for i in range(1, 11)],
    "department_name": [f"Department_{i}" for i in range(1, 11)],
    "manager_id": [random.randint(1, 300) for _ in range(10)]
}

# Convert to DataFrames
employee_df = pd.DataFrame(employee_data)
department_df = pd.DataFrame(department_data)

# Save to CSV
employee_df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\problem_3\data\employee_data.csv", index=False)
department_df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\problem_3\data\department_data.csv", index=False)

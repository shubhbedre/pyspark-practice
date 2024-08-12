import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate sales data
sales_data = {
    "transaction_id": [f"T_{i:06}" for i in range(1, 10001)],
    "customer_id": [f"C_{random.randint(1, 1000):05}" for _ in range(10000)],
    "product_id": [f"P_{random.randint(1, 500):04}" for _ in range(10000)],
    "transaction_date": [datetime(2023, 1, 1) + timedelta(hours=random.randint(0, 1000)) for _ in range(10000)],
    "quantity": [random.randint(1, 20) for _ in range(10000)],
    "price_per_unit": [round(random.uniform(10.0, 500.0), 2) for _ in range(10000)],
    "discount": [round(random.uniform(0.0, 50.0), 2) for _ in range(10000)]
}

# Generate customer data
customer_data = {
    "customer_id": [f"C_{i:05}" for i in range(1, 1001)],
    "customer_name": [f"Customer_{i}" for i in range(1, 1001)],
    "customer_email": [f"customer_{i}@example.com" for i in range(1, 1001)],
    "customer_city": [random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]) for _ in range(1000)],
    "customer_signup_date": [datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000)) for _ in range(1000)]
}

# Generate product data
product_data = {
    "product_id": [f"P_{i:04}" for i in range(1, 501)],
    "product_name": [f"Product_{i}" for i in range(1, 501)],
    "product_category": [random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"]) for _ in range(500)],
    "product_price": [round(random.uniform(10.0, 1000.0), 2) for _ in range(500)],
    "product_supplier_id": [f"S_{random.randint(1, 50):03}" for _ in range(500)]
}

# Convert to DataFrames
sales_df = pd.DataFrame(sales_data)
customer_df = pd.DataFrame(customer_data)
product_df = pd.DataFrame(product_data)

# Save to CSV
sales_df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\E-Commerce_Platform_problem\data\sales_data.csv", index=False)
customer_df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\E-Commerce_Platform_problem\data\customer_data.csv", index=False)
product_df.to_csv(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\E-Commerce_Platform_problem\data\product_data.csv", index=False)

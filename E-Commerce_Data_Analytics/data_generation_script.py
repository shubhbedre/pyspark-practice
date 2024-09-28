import random
import datetime
import pandas as pd

# Generate random customer data
customers = pd.DataFrame({
    "customer_id": range(1, 101),
    "customer_name": [f"Customer_{i}" for i in range(1, 101)],
    "city": [random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]) for _ in range(100)]
})

# Generate random product data
products = pd.DataFrame({
    "product_id": range(1, 51),
    "category": [random.choice(["Electronics", "Clothing", "Furniture", "Books", "Sports"]) for _ in range(50)],
    "price": [random.uniform(10, 1000) for _ in range(50)]
})

# Generate random order data
def random_date():
    return datetime.date(2023, random.randint(1, 12), random.randint(1, 28))

orders = pd.DataFrame({
    "order_id": range(1, 1001),
    "customer_id": [random.randint(1, 100) for _ in range(1000)],
    "product_id": [random.randint(1, 50) for _ in range(1000)],
    "order_amount": [random.uniform(10, 1000) for _ in range(1000)],
    "order_date": [random_date() for _ in range(1000)]
})

# Save to CSV files
customers.to_csv(r'data/customers.csv', index=False)
products.to_csv(r'data/products.csv', index=False)
orders.to_csv(r'data/orders.csv', index=False)

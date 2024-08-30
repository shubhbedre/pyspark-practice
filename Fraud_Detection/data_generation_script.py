import pandas as pd
import json
import random

def generate_historical_transactions(num_customers, num_transactions):
    data = {
        "transaction_id": [f"txn_{i}" for i in range(num_transactions)],
        "customer_id": [f"cust_{random.randint(1, num_customers)}" for _ in range(num_transactions)],
        "transaction_amount": [round(random.uniform(10, 10000), 2) for _ in range(num_transactions)],
        "transaction_date": pd.date_range(start='2023-01-01', periods=num_transactions, freq='H').strftime('%Y-%m-%d %H:%M:%S'),
        "merchant_category": [random.choice(["Electronics", "Groceries", "Utilities", "Clothing", "Travel"]) for _ in range(num_transactions)],
        "currency": [random.choice(["USD", "EUR", "INR", "GBP", "JPY"]) for _ in range(num_transactions)],
        "transaction_status": [random.choice(["Success", "Failed", "Pending"]) for _ in range(num_transactions)],
        "is_fraud": [random.choice([0, 1]) for _ in range(num_transactions)]
    }
    df = pd.DataFrame(data)
    df.to_csv(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Fraud_Detection\data\historical_transactions.csv', index=False)


def generate_customer_profiles(num_customers):
    data = []
    for i in range(1, num_customers + 1):
        profile = {
            "customer_id": f"cust_{i}",
            "name": f"Customer_{i}",
            "age": random.randint(18, 75),
            "gender": random.choice(["Male", "Female"]),
            "country": random.choice(["USA", "UK", "India", "Germany", "Japan"]),
            "account_balance": round(random.uniform(100, 100000), 2),
            "credit_score": random.randint(300, 850),
            "missing_value": random.choice([None, round(random.uniform(1000, 100000), 2)])
        }
        data.append(profile)
    
    with open(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Fraud_Detection\data\customer_profiles.json', 'w') as f:
        json.dump(data, f, indent=4)


def generate_streaming_transactions(num_customers, num_transactions):
    data = {
        "transaction_id": [f"str_txn_{i}" for i in range(num_transactions)],
        "customer_id": [f"cust_{random.randint(1, num_customers)}" for _ in range(num_transactions)],
        "transaction_amount": [round(random.uniform(10, 10000), 2) for _ in range(num_transactions)],
        "transaction_date": pd.date_range(start='2024-01-01', periods=num_transactions, freq='T').strftime('%Y-%m-%d %H:%M:%S'),
        "merchant_category": [random.choice(["Electronics", "Groceries", "Utilities", "Clothing", "Travel"]) for _ in range(num_transactions)],
        "currency": [random.choice(["USD", "EUR", "INR", "GBP", "JPY"]) for _ in range(num_transactions)],
        "transaction_status": [random.choice(["Success", "Failed", "Pending"]) for _ in range(num_transactions)],
    }
    df = pd.DataFrame(data)
    df.to_parquet(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Fraud_Detection\data\streaming_transactions.parquet', index=False)


generate_streaming_transactions(num_customers=1000, num_transactions=10000)
generate_customer_profiles(num_customers=1000)
generate_historical_transactions(num_customers=1000, num_transactions=50000)

import json
import random
import pandas as pd

def generate_customer_data(num_customers):
    data = []
    for i in range(1, num_customers + 1):
        customer = {
            "customer_id": f"cust_{i}",
            "name": f"Customer_{i}",
            "age": random.randint(18, 75),
            "gender": random.choice(["Male", "Female"]),
            "country": random.choice(["USA", "India", "UK", "Germany", "Australia"]),
            "signup_date": f"202{random.randint(0, 2)}-{random.randint(1, 12)}-{random.randint(1, 28)}",
            "plan_type": random.choice(["Basic", "Standard", "Premium"]),
            "monthly_fee": round(random.uniform(10, 100), 2),
            "missing_value": random.choice([None, round(random.uniform(1000, 5000), 2)])
        }
        data.append(customer)
    
    with open(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Advanced_Analytics_for_a_Telecom_Company\data\customer_data.json', 'w') as f:
        json.dump(data, f, indent=4)

def generate_usage_data(num_customers, num_records):
    data = {
        "usage_id": [f"usage_{i}" for i in range(num_records)],
        "customer_id": [f"cust_{random.randint(1, num_customers)}" for _ in range(num_records)],
        "call_duration": [round(random.uniform(1, 100), 2) for _ in range(num_records)],
        "sms_count": [random.randint(0, 50) for _ in range(num_records)],
        "data_usage": [round(random.uniform(0.1, 10), 2) for _ in range(num_records)],
        "usage_date": pd.date_range(start='2022-01-01', periods=num_records, freq='H').strftime('%Y-%m-%d %H:%M:%S'),
        "usage_type": [random.choice(["Call", "SMS", "Data"]) for _ in range(num_records)]
    }
    df = pd.DataFrame(data)
    df.to_csv(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Advanced_Analytics_for_a_Telecom_Company\data\usage_data.csv', index=False)

def generate_billing_data(num_customers, num_bills):
    data = {
        "bill_id": [f"bill_{i}" for i in range(num_bills)],
        "customer_id": [f"cust_{random.randint(1, num_customers)}" for _ in range(num_bills)],
        "billing_month": pd.date_range(start='2022-01-01', periods=num_bills, freq='M').strftime('%Y-%m'),
        "total_amount": [round(random.uniform(20, 300), 2) for _ in range(num_bills)],
        "payment_status": [random.choice(["Paid", "Unpaid", "Pending"]) for _ in range(num_bills)],
        "fraudulent": [random.choice([0, 1]) for _ in range(num_bills)]
    }
    df = pd.DataFrame(data)
    df.to_parquet(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Advanced_Analytics_for_a_Telecom_Company\data\billing_data.parquet', index=False)

generate_billing_data(num_customers=100, num_bills=1000)
generate_customer_data(num_customers=100)
generate_usage_data(num_customers=100, num_records=5000)
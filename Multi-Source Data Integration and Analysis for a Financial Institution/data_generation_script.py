import json
import random
import pandas as pd
import xml.etree.ElementTree as ET


def generate_transactions(num_customers, num_transactions):
    data = []
    for i in range(num_transactions):
        transaction = {
            "customer_id": f"cust_{random.randint(1, num_customers)}",
            "transaction_id": f"txn_{i}",
            "amount": round(random.uniform(10, 5000), 2),
            "transaction_date": pd.Timestamp('2023-01-01') + pd.to_timedelta(random.randint(0, 365), unit='d'),
            "category": random.choice(["Groceries", "Electronics", "Clothing", "Entertainment", "Utilities"]),
            "status": random.choice(["Success", "Failed", "Pending"])
        }
        data.append(transaction)
    
    with open(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Multi-Source Data Integration and Analysis for a Financial Institution\data\transactions.json', 'w') as f:
        json.dump(data, f, indent=4, default=str)


def generate_credit_scores(num_customers):
    data = {
        "customer_id": [f"cust_{i}" for i in range(1, num_customers + 1)],
        "credit_score": [random.randint(300, 850) for _ in range(num_customers)],
        "credit_utilization": [round(random.uniform(0, 1), 2) for _ in range(num_customers)],
        "credit_limit": [random.randint(500, 50000) for _ in range(num_customers)],
        "missing_value": [random.choice([None, round(random.uniform(0, 1), 2)]) for _ in range(num_customers)]
    }
    df = pd.DataFrame(data)
    df.to_csv(r'C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Multi-Source Data Integration and Analysis for a Financial Institution\data\credit_scores.csv', index=False)


def generate_customer_feedback(num_customers):
    root = ET.Element("Feedbacks")
    for i in range(1, num_customers + 1):
        feedback = ET.SubElement(root, "Feedback")
        customer_id = ET.SubElement(feedback, "CustomerID")
        customer_id.text = f"cust_{i}"
        text = ET.SubElement(feedback, "Text")
        text.text = random.choice([
            "Great service!", 
            "Poor experience, will not return.", 
            "Average support, room for improvement.", 
            "Excellent product quality!", 
            "Delivery was delayed, disappointed."
        ])
    tree = ET.ElementTree(root)
    tree.write(r"C:\Users\Shubh\Desktop\Big_Data\practice\practice_problems\Multi-Source Data Integration and Analysis for a Financial Institution\data\customer_feedback.xml")


generate_transactions(num_customers=1000, num_transactions=10000)
generate_credit_scores(num_customers=1000)
generate_customer_feedback(num_customers=1000)
import pandas as pd
import random
import faker
from datetime import datetime

# Load customer IDs from customer_info.csv
df_customers = pd.read_csv('./source/customer_info.csv')
customer_ids = df_customers['customer_id'].tolist()

# Initialize Faker
fake = faker.Faker()

# Number of records (you can increase this)
num_records = 5000000  # 5 million transactions

# Lists of possible product IDs and locations
product_ids = [f"prod_{i}" for i in range(1, 101)]  # 100 products
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']

# Generate transactions
records = []
for _ in range(num_records):
    customer_id = random.choice(customer_ids)  # Ensure valid customer_id
    purchase_date = fake.date_between(start_date='-2y', end_date='today')
    product_id = random.choice(product_ids)
    amount_spent = round(random.uniform(10.0, 500.0), 2)
    location = random.choice(locations)
    records.append([customer_id, purchase_date, product_id, amount_spent, location])

# Create DataFrame and save
df_transactions = pd.DataFrame(records, columns=['customer_id', 'purchase_date', 'product_id', 'amount_spent', 'location'])
df_transactions.to_csv('./source/customer_transactions.csv', index=False)

print("CSV file 'customer_transactions.csv' has been generated successfully.")

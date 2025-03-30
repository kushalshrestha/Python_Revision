import pandas as pd
import random
import faker
from datetime import datetime

# Initialize Faker for generating fake customer and transaction data
fake = faker.Faker()

# Number of records to generate (you can change this as needed)
num_records = 5000000

# Lists of possible customer IDs and product IDs
product_ids = [f"prod_{i}" for i in range(1, 101)]  # 100 different products
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']

# List to store the generated records
records = []

# Generate random customer transaction data
for _ in range(num_records):
    customer_id = fake.unique.uuid4()  # Generate unique customer ID
    purchase_date = fake.date_between(start_date='-2y', end_date='today')  # Random date in last 2 years
    product_id = random.choice(product_ids)  # Random product ID
    amount_spent = round(random.uniform(10.0, 500.0), 2)  # Random amount spent between $10 and $500
    location = random.choice(locations)  # Random location
    records.append([customer_id, purchase_date, product_id, amount_spent, location])

# Create a DataFrame
df = pd.DataFrame(records, columns=['customer_id', 'purchase_date', 'product_id', 'amount_spent', 'location'])

# Save the DataFrame to a CSV file
df.to_csv('./source/customer_transactions.csv', index=False)

print("CSV file 'customer_transactions.csv' has been generated successfully.")

import pandas as pd
import faker
import random

# Initialize Faker
fake = faker.Faker()

# Number of customers
num_customers = 500000  # Assuming 500K unique customers

# Generate customer info
customers = []
for _ in range(num_customers):
    customer_id = fake.unique.uuid4()
    name = fake.name()
    email = fake.email()
    age = random.randint(18, 70)  # Random age between 18 and 70
    city = fake.city()
    customers.append([customer_id, name, email, age, city])

# Create a DataFrame
df_customers = pd.DataFrame(customers, columns=['customer_id', 'name', 'email', 'age', 'city'])

# Save to CSV
df_customers.to_csv('./source/customer_info.csv', index=False)

print("CSV file 'customer_info.csv' has been generated successfully.")

import pandas as pd
import random

# Define product categories and names
categories = ['Electronics', 'Clothing', 'Home Appliances', 'Books', 'Toys']
product_names = {
    'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Smartwatch', 'Tablet'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sweater', 'Shoes'],
    'Home Appliances': ['Vacuum Cleaner', 'Microwave', 'Refrigerator', 'Washing Machine', 'Air Purifier'],
    'Books': ['Fiction Novel', 'Science Textbook', 'History Book', 'Cookbook', 'Biography'],
    'Toys': ['Lego Set', 'Puzzle', 'Doll', 'Remote Control Car', 'Board Game']
}

# Number of products
num_products = 100

# Generate product info
products = []
for i in range(1, num_products + 1):
    category = random.choice(categories)
    product_name = random.choice(product_names[category])
    price = round(random.uniform(10.0, 1000.0), 2)  # Random price between $10 and $1000
    products.append([f'prod_{i}', product_name, category, price])

# Create a DataFrame
df_products = pd.DataFrame(products, columns=['product_id', 'product_name', 'category', 'price'])

# Save to CSV
df_products.to_csv('./source/product_info.csv', index=False)

print("CSV file 'product_info.csv' has been generated successfully.")

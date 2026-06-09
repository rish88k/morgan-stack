from itertools import count
from operator import countOf
import psycopg2
import time
import random
from faker import Faker
from psycopg2.extras import execute_values
import os

# Initialize Faker
fake = Faker()

# Database Connection (matches your docker-compose mappings)
db_config = {
    "host": "postgres-rdbms",
    "port": 5432, 
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD") 
}

def get_connection():
    return psycopg2.connect(**db_config)

def generate_data():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Generate 10 initial Customers
    print("Generating customers...")

    customers = []
    for _ in range(50):
        customers.append((
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number(),
            fake.address().replace('\n', ' '),
            fake.city(),
            fake.country()
        ))

   
    insert_cust_query = """
        INSERT INTO public.customers (first_name, last_name, email, phone_number, address, city, country)
        VALUES %s RETURNING customer_id;
    """
    execute_values(cur, insert_cust_query, customers)
    customer_ids = [row[0] for row in cur.fetchall()]
    conn.commit()
    num= len(customer_ids)
    print(f"customers generated: {num}")

    # 2. Generate Accounts for those Customers
    print("Generating accounts...")
    accounts = []
    for c_id in customer_ids:
        accounts.append((
            c_id,
            fake.iban(),
            random.choice(['savings', 'checking']),
            round(random.uniform(1000, 10000), 2)
        ))
    
    insert_acc_query = """
        INSERT INTO public.accounts (customer_id, account_number, account_type, balance)
        VALUES %s RETURNING account_id;
    """
    execute_values(cur, insert_acc_query, accounts)
    account_ids = [row[0] for row in cur.fetchall()]
    conn.commit()
    num2= len(account_ids)
    print(f"no. of accounts generated: {num2}")

    # 3. Continuous Transaction Loop
    print("Starting real-time transaction simulation...")
    
    try:
        counter=0
        while True:
            acc_id = random.choice(account_ids)
            trans_type = random.choice(['deposit', 'withdrawal', 'payment'])
            amount = round(random.uniform(10, 500), 2)
            
            cur.execute("""
                INSERT INTO public.acc_transactions (account_id, transaction_type, amount, description, merchant_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (acc_id, trans_type, amount, fake.sentence(), fake.company()))
            
            conn.commit()
            print(f"Inserted {trans_type} of ${amount} for account {acc_id}")
            counter=counter+1
            
            # Wait 2 seconds before next transaction to simulate real flow
            print(f"transaction counter: {counter}")
            time.sleep(2)
            
            
    except KeyboardInterrupt:
        print(f"Simulation stopped. total transactions: {counter}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    generate_data()
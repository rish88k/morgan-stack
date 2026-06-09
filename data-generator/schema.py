import psycopg2
import os

db_config = {
    "host": "postgres-rdbms",
    "port": 5432, 
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD") 
}

def get_connection():
    return psycopg2.connect(**db_config)

def create_schema() :
    conn = get_connection()
    cur = conn.cursor()
 
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    cur.execute("DROP TABLE IF EXISTS customers CASCADE;")
    create_table_customers_query="""
        CREATE TABLE customers (
        customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone_number VARCHAR(50),
        address TEXT,
        city VARCHAR(50),
        country VARCHAR(50),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """
    cur.execute(create_table_customers_query)
    conn.commit()
    print("created customers table")

    cur.execute("DROP TABLE IF EXISTS accounts CASCADE;")
    create_table_accounts_query="""
        CREATE TABLE accounts (
        account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_id UUID REFERENCES customers(customer_id) ON DELETE CASCADE,
        account_number VARCHAR(50) UNIQUE NOT NULL,
        account_type VARCHAR(50) CHECK (account_type IN ('savings', 'checking', 'loan')),
        balance DECIMAL(15, 2) DEFAULT 0.00,
        currency VARCHAR(3) DEFAULT 'USD',
        status VARCHAR(15) DEFAULT 'active',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """
    cur.execute(create_table_accounts_query)
    conn.commit()
    print("created accounts table")

    cur.execute("DROP TABLE IF EXISTS acc_transactions CASCADE;")
    create_table_transactions_query="""
        CREATE TABLE acc_transactions (
        transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        account_id UUID REFERENCES accounts(account_id) ON DELETE CASCADE,
        transaction_type VARCHAR(30) CHECK (transaction_type IN ('deposit', 'withdrawal', 'transfer', 'payment')),
        amount DECIMAL(15, 2) NOT NULL,
        transaction_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        description TEXT,
        merchant_name VARCHAR(100)
        );
    """
    cur.execute(create_table_transactions_query)
    conn.commit()
    print("created acc_transactions table")
    cur.close()
    conn.close()

    print("finished the fucking job, mate")

if __name__ == "__main__":
    create_schema()

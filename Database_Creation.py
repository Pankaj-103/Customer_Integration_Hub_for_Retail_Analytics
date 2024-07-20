import sqlite3
import pandas as pd

def insert_into_sqlite():
    # Connect to SQLite database (it will create the database file if it doesn't exist)
    conn = sqlite3.connect('/opt/airflow/data/Retail_data.db')

    # Create a cursor object
    cursor = conn.cursor()

    # Create table
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS Retail_data (
        Row_ID INTEGER,
        Order_ID TEXT,
        ORDER_DATE TEXT,
        SHIP_DATE TEXT,
        SHIP_MODE TEXT,
        CUSTOMER_ID TEXT,
        CUSTOMER_NAME TEXT,
        SEGMENT TEXT,
        CITY TEXT,
        CUST_STATE TEXT,
        REGION TEXT,
        PRODUCT_ID TEXT,
        CATEGORY TEXT,
        SUB_CATEGORY TEXT,
        PRODUCT_NAME TEXT,
        SALES REAL,
        QUANTITY INTEGER,
        DISCOUNT REAL,
        PROFIT REAL,
        PROFIT_STATUS INTEGER
    );
    '''

    cursor.execute(create_table_query)

    # Commit the changes
    conn.commit()

    # Read CSV file into pandas DataFrame
    df = pd.read_csv('/opt/airflow/data/Combined_data.csv')

    # Insert DataFrame records into SQLite database
    df.to_sql('Retail_data', conn, if_exists='append', index=False)

    # Close the connection
    conn.close()

# Execute the function to test
if __name__ == "__main__":
    insert_into_sqlite()
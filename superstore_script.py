import sqlite3
import pandas as pd

# # Function to connect to the database
# db_path = "/Users/ishaandawra/Desktop/Airflow_Skydive/dags/data/Retail_data.db"
# def connect_to_database(db_path):
#     return sqlite3.connect(db_path)

# Function to execute a query and return a DataFrame
def execute_query(conn, query):
    return pd.read_sql_query(query, conn)

# Function to merge dataframes
def merge_dataframes(dfs, on, how='outer'):
    from functools import reduce
    return reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)

# Function to export dataframe to CSV
def export_to_csv(df, file_name):
    export_path = "/opt/airflow/dags/" + file_name  # Absolute path
    df.to_csv(export_path, index=False)

# Main function to orchestrate operations

def main():
    
    conn = sqlite3.connect('/opt/airflow/data/Retail_data.db')

    queries = [
        "SELECT CUSTOMER_ID,CUSTOMER_NAME,CITY,REGION,SEGMENT FROM Retail_data GROUP BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID,SUM(SALES) AS Total_expenditure FROM Retail_data GROUP BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID, COUNT(DISTINCT Order_ID) AS total_orders FROM Retail_data GROUP BY CUSTOMER_ID;",

        " SELECT CUSTOMER_ID, CASE WHEN Order_Count > 1 THEN 'Repeated' ELSE 'Not Repeated' END AS Repeating_customers FROM (SELECT CUSTOMER_ID,COUNT(DISTINCT Order_ID) AS Order_Count FROM Retail_data GROUP BY CUSTOMER_ID); ",

        "SELECT CUSTOMER_ID, SUM(Profit) AS Total_Profit FROM Retail_data WHERE  CUSTOMER_ID NOT IN ( SELECT DISTINCT CUSTOMER_ID FROM Retail_data WHERE Profit < 0) GROUP BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID,SUM(Profit) AS Profits FROM Retail_data GROUP BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID,SUM(SALES)/COUNT(DISTINCT Order_ID) AS Avg_Spending_on_each_Order FROM Retail_data GROUP BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID,CATEGORY AS Prefered_category, SUB_CATEGORY AS prefered_sub_category FROM (SELECT CUSTOMER_ID,CATEGORY, SUB_CATEGORY, ROW_NUMBER() OVER(PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) AS randknumber FROM  Retail_data GROUP BY  CUSTOMER_ID, CATEGORY, SUB_CATEGORY) AS temp WHERE randknumber = 1 ORDER BY CUSTOMER_ID;",

        "SELECT CUSTOMER_ID, CASE WHEN MIN(DISCOUNT) > 0 THEN 'always' ELSE 'not always' END AS buying_only_discounted FROM  Retail_data GROUP BY CUSTOMER_ID;",

        "WITH RankedShipModes AS (SELECT CUSTOMER_ID,SHIP_MODE, COUNT(*) AS ShipMode_Count, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) AS rank_number FROM Retail_data GROUP BY CUSTOMER_ID, SHIP_MODE) SELECT CUSTOMER_ID,SHIP_MODE, ShipMode_Count FROM RankedShipModes WHERE rank_number = 1;",

        "WITH RankedProducts AS (SELECT CUSTOMER_ID,PRODUCT_NAME,COUNT(*) AS Most_brought_product,ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY COUNT(*) DESC) AS rank_number FROM  Retail_data GROUP BY CUSTOMER_ID,PRODUCT_NAME) SELECT CUSTOMER_ID,PRODUCT_NAME AS Most_brought_products,Most_brought_product AS quantity_of_products FROM RankedProducts WHERE  rank_number = 1;",

    ]
    
    # Execute queries and store results in a list of dataframes
    dataframes = [execute_query(conn, q) for q in queries]
    
    # Merge dataframes
    final_data = merge_dataframes(dataframes, 'CUSTOMER_ID')
    
    # Export to CSV
    export_to_csv(final_data, 'final_data.csv')
    
    print('Data processing complete and exported to CSV.')

# Print the refactored code to confirm
print(main.__code__.co_code)
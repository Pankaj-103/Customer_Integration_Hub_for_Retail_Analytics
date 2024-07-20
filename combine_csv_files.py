import pandas as pd
import os

# Function to export dataframe to CSV
def export_to_csv(df, file_name):
    export_path = "/opt/airflow/data/" + file_name  # Absolute path
    df.to_csv(export_path, index=False)


def combine_csv_files():
    # Define file paths
    file1 = '/opt/airflow/data/Superstore_Copy_2017.csv'
    file2 = '/opt/airflow/data/superstore_2019.csv'

    # Check if files exist
    if not (os.path.exists(file1) and os.path.exists(file2)):
        raise FileNotFoundError("CSV files not found!")

    # Read CSV files into DataFrames
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    # Combine DataFrames
    combined_df = pd.concat([df1, df2], ignore_index=True)

    # Rename columns
    combined_df.columns = [
        'Row_ID', 'Order_ID', 'ORDER_DATE', 'SHIP_DATE', 'SHIP_MODE', 'CUSTOMER_ID',
        'CUSTOMER_NAME', 'SEGMENT', 'CITY', 'CUST_STATE', 'REGION', 'PRODUCT_ID',
        'CATEGORY', 'SUB_CATEGORY', 'PRODUCT_NAME', 'SALES', 'QUANTITY', 'DISCOUNT',
        'PROFIT'
    ]

    # Data preprocessing
    df = combined_df

    # Check for missing values
    print(df.isna().any())
    # print(df.isnull().any())
    print(df.isnull().sum())

    # Check for duplicates and outliers
    print(df.duplicated().value_counts())
    print(df[df['DISCOUNT'] > 1])
    print(df[df['DISCOUNT'] < 0])
    # print(df[df['PROFIT'] < 0])

    # Create PROFIT_STATUS column
    # df['PROFIT_STATUS'] = df['PROFIT'].apply(lambda x: 1 if x >= 0 else 0)

    # Summary statistics
    print(df.describe())

    export_to_csv(df, 'Combined_data.csv')

    # Save combined data to CSV
    # combined_file = '/opt/airflow/data/Combined_data.csv'
    # df.to_csv(combined_file, index=False)
    # print("Combined file is saved in data")
    # print(f"Combined data saved to {combined_file}")

# Execute the function to test
if __name__ == "__main__":
    combine_csv_files()




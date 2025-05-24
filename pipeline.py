import pandas as pd
import numpy as np

# Define the file paths for your sales reports
# Make sure these CSV files are in the same directory as your pipeline.py script
amazon_report_path = 'Amazon Sale Report.csv'
international_report_path = 'International sale Report.csv'
sale_report_path = 'Sale Report.csv'

# print("--- Exploring Amazon Sale Report ---")
# try:
#     # Read the Amazon Sale Report CSV file
#     df_amazon = pd.read_csv(amazon_report_path)

#     # Strip whitespace from column headers
#     df_amazon.columns = df_amazon.columns.str.strip()

#     # Drop unnecessary columns if they exist
#     print("\nDropping unnecessary columns from Amazon Sale Report:")
#     df_amazon.drop(columns=['Unnamed: 22', 'index'], inplace=True, errors='ignore')

#     # Rename columns for consistency
#     print("\nRenaming columns in Amazon Sale Report:")
    
#     df_amazon.rename(columns={
#         "Order ID": "order_id",
#         "Date": "order_date",
#         "Qty": "quantity",
#         "Amount": "amount", # Keeping 'amount' for now, not 'total_amount' as per your code
#         "SKU": "sku",
#         "ship-city": "ship_city",
#         "ship-state": "ship_state",
#         "ship-postal-code": "ship_postal_code",
#         "ship-country": "ship_country",
#         "Sales Channel": "sales_channel", # This was `Sales Channel` (with space) to `sales_channel`
#         "Style": "product_style",
#         "Category": "product_category",
#         "Size": "product_size",
#         "ASIN": "product_asin",
#         "promotion-ids": "promotion_ids",
#         "B2B": "is_b2b",
#         "fulfilled-by": "fulfillment_by", # <--- CORRECTED THIS ONE!
#         "Courier Status": "courier_status",
#         "currency": "currency",
#         "Status": "order_status",
#         "Fulfilment": "fulfillment_type",
#         "ship-service-level": "ship_service_level",
#     }, inplace=True, errors='ignore')

#     # Convert order_date to datetime and ship_postal_code to object (string)
#     print("\nConverting data types in Amazon Sale Report:")
#     df_amazon['order_date'] = pd.to_datetime(df_amazon['order_date'], errors='coerce')
#     df_amazon['ship_postal_code'] = df_amazon['ship_postal_code'].astype(str)

#     print("\nFirst 5 rows of Amazon Sale Report:")
#     print(df_amazon.head())

#     print("\nInfo of Amazon Sale Report:")
#     df_amazon.info()
    
#     print("\nMissing values in Amazon Sale Report:")
#     print(df_amazon.isnull().sum())
#     print("-" * 50) # Separator for readability
#     print(df_amazon.columns.tolist())

# except FileNotFoundError:
#     print(f"Error: '{amazon_report_path}' not found. Please ensure the file is in the correct directory.")
# except Exception as e:
#     print(f"An error occurred while reading '{amazon_report_path}': {e}")


# print("\n--- Exploring International Sale Report ---")
# try:
#     df_international = pd.read_csv(international_report_path)

#     # Drop unnecessary columns if they exist
#     df_international.drop(columns=['index'], inplace=True, errors='ignore')


#     # Rename columns for consistency
#     df_international.rename(columns={
#         "DATE": "order_date",
#         "Months": "order_month",
#         "CUSTOMER": "customer_name",
#         "Style": "product_style",
#         "SKU": "sku",
#         "Size": "product_size",
#         "PCS": "quantity",
#         "RATE": "unit_price",
#         "GROSS AMT": "total_amount",
#     }, inplace=True, errors='ignore')

#     # Convert data types for df_international
#     df_international['order_date'] = pd.to_datetime(df_international['order_date'], format='%m-%d-%y', errors='coerce')
#     df_international['quantity'] = pd.to_numeric(df_international['quantity'], errors='coerce').fillna(0).astype(np.int64)
#     df_international['unit_price'] = pd.to_numeric(df_international['unit_price'], errors='coerce')
#     df_international['total_amount'] = pd.to_numeric(df_international['total_amount'], errors='coerce')

#     print("\nFirst 5 rows of International Sale Report:")
#     print(df_international.head())

#     print("\nInfo of International Sale Report:")
#     df_international.info()
 
#     print("\nMissing values in International Sale Report:")
#     print(df_international.isnull().sum())
    
#     print("-" * 50)

# except FileNotFoundError:
#     print(f"Error: '{international_report_path}' not found. Please ensure the file is in the correct directory.")
# except Exception as e:
#     print(f"An error occurred while reading '{international_report_path}': {e}")


print("\n--- Exploring Sale Report ---")
try:
    df_sale = pd.read_csv(sale_report_path)
    
    df_sale.drop(columns=['index'], inplace=True, errors='ignore')
    
    df_sale.rename(columns={
        "SKU Code": "sku",
        "Design No.": "design_no",
        "Stock": "current_stock",
        "Category": "product_category",
        "Size": "product_size",
        "Color": "product_color",
    }, inplace=True, errors='ignore')
    
    # Conver current_stock to Int64
    df_sale['current_stock'] = pd.to_numeric(df_sale['current_stock'], errors='coerce').fillna(0).astype(np.int64)    
    
    print("\nFirst 5 rows of Sale Report:")
    print(df_sale.head())
    
    print("\nInfo of Sale Report:")
    df_sale.info()
    
    print("\nMissing values in Sale Report:")
    print(df_sale.isnull().sum())
    
    print("-" * 50)

except FileNotFoundError:
    print(f"Error: '{sale_report_path}' not found. Please ensure the file is in the correct directory.")
except Exception as e:
    print(f"An error occurred while reading '{sale_report_path}': {e}")
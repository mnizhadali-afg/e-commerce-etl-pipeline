import pandas as pd
import numpy as np

def clean_dataframe(df, drop_cols=None, rename_dict=None, dtype_conversions=None):
    """
    Applies common cleaning steps to a DataFrame:
    - Strips whitespace from column names.
    - Drops specified columns.
    - Renames columns.
    - Converts specified data types.
    """
    df.columns = df.columns.str.strip()
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True, errors='ignore')
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True, errors='ignore')
    if dtype_conversions:
        for col, func in dtype_conversions.items():
            if col in df.columns:
                df[col] = func(df[col])
    return df

# --- Configuration for Data Cleaning ---
amazon_rename = {
    "Order ID": "order_id", "Date": "order_date", "Qty": "quantity", "Amount": "amount",
    "SKU": "sku", "ship-city": "ship_city", "ship-state": "ship_state",
    "ship-postal-code": "ship_postal_code", "ship-country": "ship_country",
    "Sales Channel": "sales_channel", "Style": "product_style", "Category": "product_category",
    "Size": "product_size", "ASIN": "product_asin", "promotion-ids": "promotion_ids",
    "B2B": "is_b2b", "fulfilled-by": "fulfillment_by", "Courier Status": "courier_status",
    "currency": "currency", "Status": "order_status", "Fulfilment": "fulfillment_type",
    "ship-service-level": "ship_service_level",
}
amazon_types = {
    'order_date': lambda x: pd.to_datetime(x, errors='coerce'),
    'ship_postal_code': lambda x: x.astype(str)
}

international_rename = {
    "DATE": "order_date", "Months": "order_month", "CUSTOMER": "customer_name",
    "Style": "product_style", "SKU": "sku", "Size": "product_size", "PCS": "quantity",
    "RATE": "unit_price", "GROSS AMT": "total_amount",
}
international_types = {
    'order_date': lambda x: pd.to_datetime(x, format='%m-%d-%y', errors='coerce'),
    'quantity': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64),
    'unit_price': lambda x: pd.to_numeric(x, errors='coerce'),
    'total_amount': lambda x: pd.to_numeric(x, errors='coerce')
}

sale_rename = {
    "SKU Code": "sku", "Design No.": "design_no", "Stock": "current_stock",
    "Category": "product_category", "Size": "product_size", "Color": "product_color",
}
sale_types = {
    'current_stock': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64)
}

amazon_report_path = 'Amazon Sale Report.csv'
international_report_path = 'International sale Report.csv'
sale_report_path = 'Sale Report.csv'

# --- Data Loading and Initial Cleaning ---

print("--- Starting Data Loading and Initial Cleaning ---")

df_amazon = pd.DataFrame()
df_international = pd.DataFrame()
df_sale = pd.DataFrame()

try:
    df_amazon = pd.read_csv(amazon_report_path)
    df_amazon = clean_dataframe(
        df_amazon,
        drop_cols=['Unnamed: 22', 'index'],
        rename_dict=amazon_rename,
        dtype_conversions=amazon_types
    )
    print(f"Loaded and cleaned Amazon Sale Report. Shape: {df_amazon.shape}")
except FileNotFoundError:
    print(f"Error: '{amazon_report_path}' not found. Please ensure the file is in the correct directory.")
    exit() # Exit if a critical file is missing
except Exception as e:
    print(f"An error occurred while reading '{amazon_report_path}': {e}")
    exit()

try:
    df_international = pd.read_csv(international_report_path)
    df_international = clean_dataframe(
        df_international,
        drop_cols=['index'],
        rename_dict=international_rename,
        dtype_conversions=international_types
    )
    print(f"Loaded and cleaned International Sale Report. Shape: {df_international.shape}")
except FileNotFoundError:
    print(f"Error: '{international_report_path}' not found. Please ensure the file is in the correct directory.")
    exit()
except Exception as e:
    print(f"An error occurred while reading '{international_report_path}': {e}")
    exit()

try:
    df_sale = pd.read_csv(sale_report_path)
    df_sale = clean_dataframe(
        df_sale,
        drop_cols=['index'],
        rename_dict=sale_rename,
        dtype_conversions=sale_types
    )
    initial_sale_rows = len(df_sale)
    df_sale.drop_duplicates(subset='sku', keep='first', inplace=True)
    dropped_skus_count = initial_sale_rows - len(df_sale)
    print(f"Loaded and cleaned Sale Report (Product Master). Dropped {dropped_skus_count} duplicate SKUs. New shape: {df_sale.shape}")
except FileNotFoundError:
    print(f"Error: '{sale_report_path}' not found. Please ensure the file is in the correct directory.")
    exit()
except Exception as e:
    print(f"An error occurred while reading '{sale_report_path}': {e}")
    exit()

print("--- Data Loading and Initial Cleaning Complete ---")

# --- Data Integration ---
print("\n--- Starting Data Integration ---")

df_combined_sales = pd.concat([df_amazon, df_international], ignore_index=True)
print(f"Combined Amazon and International sales. Combined shape: {df_combined_sales.shape}")

df_final_sales = pd.merge(df_combined_sales, df_sale, on='sku', how='left')
print(f"Merged combined sales with Sale Report (product master). Final shape: {df_final_sales.shape}")

# Refine Merged Data: Handle duplicate column names
df_final_sales.drop(columns=['product_category_x', 'product_size_x'], inplace=True, errors='ignore')
df_final_sales.rename(columns={
    'product_category_y': 'product_category',
    'product_size_y': 'product_size'
}, inplace=True, errors='ignore')
print("Resolved conflicting column names ('product_category', 'product_size') post-merge.")

print("--- Data Integration Complete ---")

# --- Further Cleaning and Data Quality for df_final_sales ---
print("\n--- Starting Final Cleaning and Data Quality Assurance ---")

# 1. Handle missing 'sku' values
initial_sku_missing = df_final_sales['sku'].isnull().sum()
if initial_sku_missing > 0:
    df_final_sales.dropna(subset=['sku'], inplace=True)
    print(f"Dropped {initial_sku_missing} rows with missing 'sku'.")

# 2. Convert 'is_b2b' to boolean
df_final_sales['is_b2b'] = df_final_sales['is_b2b'].apply(lambda x: True if str(x).lower() == 'true' else False)

# 3. Handle missing numerical values (amounts, prices, current_stock)
numerical_cols_to_fill_zero = ['amount', 'unit_price', 'total_amount', 'current_stock']
for col in numerical_cols_to_fill_zero:
    if col in df_final_sales.columns:
        missing_count = df_final_sales[col].isnull().sum()
        if missing_count > 0:
            df_final_sales[col].fillna(0, inplace=True)
df_final_sales['amount'] = pd.to_numeric(df_final_sales['amount'], errors='coerce')
df_final_sales['unit_price'] = pd.to_numeric(df_final_sales['unit_price'], errors='coerce')
df_final_sales['total_amount'] = pd.to_numeric(df_final_sales['total_amount'], errors='coerce')
df_final_sales['current_stock'] = df_final_sales['current_stock'].astype(np.int64)
print("Handled missing numerical values and ensured correct dtypes.")

# 4. Handle missing `order_date`
initial_order_date_missing = df_final_sales['order_date'].isnull().sum()
if initial_order_date_missing > 0:
    df_final_sales.dropna(subset=['order_date'], inplace=True)
    print(f"Dropped {initial_order_date_missing} rows with missing 'order_date'.")
df_final_sales['order_date'] = pd.to_datetime(df_final_sales['order_date'], errors='coerce')

# 5. Handle missing categorical columns with 'Unknown'
categorical_cols_to_fill_unknown = [
    'product_category', 'product_size', 'product_color', 'design_no',
    'courier_status', 'currency', 'fulfillment_by', 'order_month',
    'customer_name', 'product_style', 'order_id', 'order_status',
    'fulfillment_type', 'sales_channel', 'ship_service_level',
    'product_asin', 'ship_city', 'ship_state', 'ship_postal_code',
    'ship_country', 'promotion_ids'
]
for col in categorical_cols_to_fill_unknown:
    if col in df_final_sales.columns:
        df_final_sales[col].fillna('Unknown', inplace=True)
print("Filled remaining missing categorical values with 'Unknown'.")

print("--- Final Cleaning and Data Quality Assurance Complete ---")

# --- Feature Engineering ---
print("\n--- Starting Feature Engineering ---")

# Ensure order_date is datetime type before feature extraction
df_final_sales['order_date'] = pd.to_datetime(df_final_sales['order_date'], errors='coerce')

# Time-based features
df_final_sales['order_year'] = df_final_sales['order_date'].dt.year
df_final_sales['order_month_num'] = df_final_sales['order_date'].dt.month
df_final_sales['order_day_of_week'] = df_final_sales['order_date'].dt.dayofweek
df_final_sales['order_hour'] = df_final_sales['order_date'].dt.hour

# Calculate total_price
df_final_sales['quantity'] = pd.to_numeric(df_final_sales['quantity'], errors='coerce').fillna(0)
df_final_sales['unit_price'] = pd.to_numeric(df_final_sales['unit_price'], errors='coerce').fillna(0)
df_final_sales['total_price'] = df_final_sales['quantity'] * df_final_sales['unit_price']
print("Engineered time-based features and calculated 'total_price'.")

print("--- Feature Engineering Complete ---")

# --- Final Output Status ---
print("\n--- E-commerce Data Pipeline Completed Successfully ---")
print(f"Final DataFrame shape: {df_final_sales.shape}")
print("First 5 rows of the final cleaned and enriched DataFrame:")
print(df_final_sales.head())
print("\nTotal missing values in the final DataFrame (should be 0 for critical columns):")
print(df_final_sales.isnull().sum())
print("\nInfo of the final DataFrame:")
df_final_sales.info()
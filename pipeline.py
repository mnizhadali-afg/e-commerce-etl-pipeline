import pandas as pd
import numpy as np

def clean_dataframe(df, drop_cols=None, rename_dict=None, dtype_conversions=None):
    # Strip whitespace from column headers
    df.columns = df.columns.str.strip()
    # Drop unnecessary columns
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True, errors='ignore')
    # Rename columns
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True, errors='ignore')
    # Convert data types
    if dtype_conversions:
        for col, func in dtype_conversions.items():
            if col in df.columns:
                df[col] = func(df[col])
    return df

# Amazon Sale Report cleaning
amazon_rename = {
    "Order ID": "order_id",
    "Date": "order_date",
    "Qty": "quantity",
    "Amount": "amount",
    "SKU": "sku",
    "ship-city": "ship_city",
    "ship-state": "ship_state",
    "ship-postal-code": "ship_postal_code",
    "ship-country": "ship_country",
    "Sales Channel": "sales_channel",
    "Style": "product_style",
    "Category": "product_category",
    "Size": "product_size",
    "ASIN": "product_asin",
    "promotion-ids": "promotion_ids",
    "B2B": "is_b2b",
    "fulfilled-by": "fulfillment_by",
    "Courier Status": "courier_status",
    "currency": "currency",
    "Status": "order_status",
    "Fulfilment": "fulfillment_type",
    "ship-service-level": "ship_service_level",
}
amazon_types = {
    'order_date': lambda x: pd.to_datetime(x, errors='coerce'),
    'ship_postal_code': lambda x: x.astype(str)
}

international_rename = {
    "DATE": "order_date",
    "Months": "order_month",
    "CUSTOMER": "customer_name",
    "Style": "product_style",
    "SKU": "sku",
    "Size": "product_size",
    "PCS": "quantity",
    "RATE": "unit_price",
    "GROSS AMT": "total_amount",
}
international_types = {
    'order_date': lambda x: pd.to_datetime(x, format='%m-%d-%y', errors='coerce'),
    'quantity': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64),
    'unit_price': lambda x: pd.to_numeric(x, errors='coerce'),
    'total_amount': lambda x: pd.to_numeric(x, errors='coerce')
}

sale_rename = {
    "SKU Code": "sku",
    "Design No.": "design_no",
    "Stock": "current_stock",
    "Category": "product_category",
    "Size": "product_size",
    "Color": "product_color",
}
sale_types = {
    'current_stock': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64)
}

amazon_report_path = 'Amazon Sale Report.csv'
international_report_path = 'International sale Report.csv'
sale_report_path = 'Sale Report.csv'

print("--- Exploring Amazon Sale Report ---")
try:
    df_amazon = pd.read_csv(amazon_report_path)
    df_amazon = clean_dataframe(
        df_amazon,
        drop_cols=['Unnamed: 22', 'index'],
        rename_dict=amazon_rename,
        dtype_conversions=amazon_types
    )
    print(df_amazon.head())
    df_amazon.info()
    print(df_amazon.isnull().sum())
except FileNotFoundError:
    print(f"Error: '{amazon_report_path}' not found. Please ensure the file is in the correct directory.")
except Exception as e:
    print(f"An error occurred while reading '{amazon_report_path}': {e}")

print("\n--- Exploring International Sale Report ---")
try:
    df_international = pd.read_csv(international_report_path)
    df_international = clean_dataframe(
        df_international,
        drop_cols=['index'],
        rename_dict=international_rename,
        dtype_conversions=international_types
    )
    print(df_international.head())
    df_international.info()
    
    print(df_international.isnull().sum())
except FileNotFoundError:
    print(f"Error: '{international_report_path}' not found. Please ensure the file is in the correct directory.")
except Exception as e:
    print(f"An error occurred while reading '{international_report_path}': {e}")

print("\n--- Exploring Sale Report ---")
try:
    df_sale = pd.read_csv(sale_report_path)
    df_sale = clean_dataframe(
        df_sale,
        drop_cols=['index'], # Assuming 'index' might still be there on initial read
        rename_dict=sale_rename,
        dtype_conversions=sale_types
    )

    # --- Investigating SKU in Sale Report (df_sale) - BEFORE dropping duplicates ---
    print("\n--- Investigating SKU in Sale Report (df_sale) (BEFORE DUPLICATE DROP) ---")
    print(f"Total unique SKUs in df_sale (before drop): {df_sale['sku'].nunique()}")
    print(f"Total rows in df_sale (before drop): {len(df_sale)}")
    # Check for duplicate SKUs in df_sale
    duplicate_skus_before_drop = df_sale[df_sale.duplicated(subset=['sku'], keep=False)]
    print(f"Number of duplicate SKUs (based on 'sku' column) in df_sale (before drop): {len(duplicate_skus_before_drop)}")
    if not duplicate_skus_before_drop.empty:
        print("Example duplicate SKUs in df_sale (before drop):")
        print(duplicate_skus_before_drop.sort_values('sku').head(10))
    print("----------------------------------------------------------------------")

    # Now, drop the duplicates, this is the solution to the merge issue
    print("\nDropping duplicate SKUs in df_sale to ensure unique product master data...")
    df_sale.drop_duplicates(subset='sku', keep='first', inplace=True) # Apply inplace=True here

    print("\n--- Investigating SKU in Sale Report (df_sale) (AFTER DUPLICATE DROP) ---")
    print(f"Total unique SKUs in df_sale (after drop): {df_sale['sku'].nunique()}")
    print(f"Total rows in df_sale (after drop): {len(df_sale)}")
    duplicate_skus_after_drop = df_sale[df_sale.duplicated(subset=['sku'], keep=False)]
    print(f"Number of duplicate SKUs (based on 'sku' column) in df_sale (after drop): {len(duplicate_skus_after_drop)}")
    if not duplicate_skus_after_drop.empty:
        print("Example duplicate SKUs in df_sale (after drop):")
        print(duplicate_skus_after_drop.sort_values('sku').head(10))
    print("---------------------------------------------------------------------")


    print(df_sale.head())
    df_sale.info()
    print(df_sale.isnull().sum())
except FileNotFoundError:
    print(f"Error: '{sale_report_path}' not found. Please ensure the file is in the correct directory.")
except Exception as e:
    print(f"An error occurred while reading '{sale_report_path}': {e}")

# Data Integration
df_combined_sales = pd.concat([df_amazon, df_international], ignore_index=True)
print(df_combined_sales.head())
df_final_sales = pd.merge(df_combined_sales, df_sale, on='sku', how='left')
# ... (code before this was pd.merge and df_final_sales.head())

# --- Refine Merged Data: Handle duplicate column names ---
print("\nRefining Merged Data: Handling column name conflicts...")
# Drop the redundant _x columns (from df_combined_sales).
# We prefer the product details from df_sale, so _x versions are less authoritative.
df_final_sales.drop(columns=['product_category_x', 'product_size_x'], inplace=True, errors='ignore')

# Rename the _y columns (from df_sale) back to their cleaner names.
df_final_sales.rename(columns={
    'product_category_y': 'product_category',
    'product_size_y': 'product_size'
}, inplace=True, errors='ignore')

print("\nFirst 5 rows of Final Sales Data (after renaming columns):")
print(df_final_sales.head())
print("\nInfo of Final Sales Data (after renaming columns):")
df_final_sales.info() # Make sure this is present and its full output is captured
print("\nMissing values in Final Sales Data (after renaming columns):")
print(df_final_sales.isnull().sum()) # Make sure this is present and its full output is captured
print("-" * 50)

# ... (code for df_final_sales merge and rename ends here) ...

# --- Phase 4: Further Cleaning and Data Quality for df_final_sales ---
print("\n--- Phase 4: Further Cleaning and Data Quality for df_final_sales ---")

# 1. Handle missing 'sku' values
# Let's count them and then decide a strategy. For now, we'll just log and maybe drop.
initial_sku_missing = df_final_sales['sku'].isnull().sum()
print(f"Initial missing SKUs in df_final_sales: {initial_sku_missing}")

# Decision: For now, let's drop rows where SKU is missing, as it's a primary identifier.
# We cannot enrich these rows with product info anyway.
if initial_sku_missing > 0:
    df_final_sales.dropna(subset=['sku'], inplace=True)
    print(f"Dropped {initial_sku_missing} rows with missing 'sku'.")

# 2. Convert 'is_b2b' to boolean
# Check current unique values first to see if there are non-boolean strings
print(f"\nUnique values in 'is_b2b' before conversion: {df_final_sales['is_b2b'].unique()}")
# Convert 'is_b2b' to boolean type. Handle potential string 'True'/'False' or other values.
# Common approach: map string representations to boolean.
# Assuming 'True' string means True, anything else (including NaN) means False.
df_final_sales['is_b2b'] = df_final_sales['is_b2b'].apply(lambda x: True if str(x).lower() == 'true' else False)
# Check type after conversion
print(f"Dtype of 'is_b2b' after conversion: {df_final_sales['is_b2b'].dtype}")


# 3. Handle missing numerical values (amounts, prices, current_stock)
# Strategy: Fill with 0 if missing value means 0 quantity/amount.
# For 'current_stock', which came from df_sale, it might be NaN if no SKU match.
# Let's consider 0 as a valid fill for these.
numerical_cols_to_fill_zero = ['amount', 'unit_price', 'total_amount', 'current_stock']
for col in numerical_cols_to_fill_zero:
    if col in df_final_sales.columns:
        missing_count = df_final_sales[col].isnull().sum()
        if missing_count > 0:
            df_final_sales[col].fillna(0, inplace=True)
            print(f"Filled {missing_count} missing values in '{col}' with 0.")

# After filling, ensure correct numeric dtypes
df_final_sales['amount'] = pd.to_numeric(df_final_sales['amount'], errors='coerce')
df_final_sales['unit_price'] = pd.to_numeric(df_final_sales['unit_price'], errors='coerce')
df_final_sales['total_amount'] = pd.to_numeric(df_final_sales['total_amount'], errors='coerce')
# current_stock should already be int64 from df_sale cleaning, but re-assert after fillna
df_final_sales['current_stock'] = df_final_sales['current_stock'].astype(np.int64)


# 4. Handle missing `order_date`
# For now, let's drop rows where `order_date` is missing as it's fundamental for time-series analysis.
initial_order_date_missing = df_final_sales['order_date'].isnull().sum()
print(f"\nInitial missing 'order_date' in df_final_sales: {initial_order_date_missing}")
if initial_order_date_missing > 0:
    df_final_sales.dropna(subset=['order_date'], inplace=True)
    print(f"Dropped {initial_order_date_missing} rows with missing 'order_date'.")

# 5. Handle missing categorical columns with 'Unknown' or similar
# For product_category, product_size, product_color, design_no which come from df_sale,
# missing values usually mean no match was found for that SKU, or the data was genuinely missing.
# Filling with 'Unknown' is a common strategy to retain rows.
categorical_cols_to_fill_unknown = ['product_category', 'product_size', 'product_color', 'design_no',
                                    'courier_status', 'currency', 'fulfillment_by', 'order_month',
                                    'customer_name', 'product_style'] # Include style as it had a few nulls
for col in categorical_cols_to_fill_unknown:
    if col in df_final_sales.columns:
        missing_count = df_final_sales[col].isnull().sum()
        if missing_count > 0:
            df_final_sales[col].fillna('Unknown', inplace=True)
            print(f"Filled {missing_count} missing values in '{col}' with 'Unknown'.")

# Final check after this phase
print("\n--- Final Status of df_final_sales after Phase 4 Cleaning ---")
print("\nFirst 5 rows:")
print(df_final_sales.head())
print("\nInfo:")
df_final_sales.info()
print("\nMissing values:")
print(df_final_sales.isnull().sum())
print("-" * 50)

# ... (End of Phase 4 cleaning code) ...

# --- Phase 5: Final Handling of Remaining Missing Values ---
print("\n--- Phase 5: Final Handling of Remaining Missing Values ---")

# 1. Handle remaining core identifying / descriptive categorical columns with 'Unknown'
# These columns primarily originate from the Amazon report but might be NaN for International sales records.
remaining_categorical_cols_to_fill_unknown = [
    'order_id', 'order_status', 'fulfillment_type', 'sales_channel',
    'ship_service_level', 'product_asin', 'ship_city', 'ship_state',
    'ship_postal_code', 'ship_country', 'promotion_ids'
]

for col in remaining_categorical_cols_to_fill_unknown:
    if col in df_final_sales.columns:
        missing_count = df_final_sales[col].isnull().sum()
        if missing_count > 0:
            df_final_sales[col].fillna('Unknown', inplace=True)
            print(f"Filled {missing_count} missing values in '{col}' with 'Unknown'.")

# Final check after this phase
print("\n--- Final Status of df_final_sales after Phase 5 Cleaning ---")
print("\nFirst 5 rows:")
print(df_final_sales.head())
print("\nInfo:")
df_final_sales.info()
print("\nMissing values:")
print(df_final_sales.isnull().sum())
print("-" * 50)
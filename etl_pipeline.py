#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import logging
import sys
from sqlalchemy import create_engine
import os # For environment variables
from urllib.parse import quote_plus
from dotenv import load_dotenv
import hashlib

# Set up logging
# Configure logging to output to console (stdout) and a file (optional)
# You can change the level to logging.DEBUG to see more detailed messages during development
logging.basicConfig(
    level=logging.INFO, # Change to logging.DEBUG for more verbose output
    format='%(asctime)s - %(levelname)s - %(message)s', 
    handlers=[
        logging.StreamHandler(sys.stdout), # Output to console
        # logging.FileHandler('pipeline_log.log') # Uncomment to also log to a file
    ]
)


# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# File paths (UPDATED TO USE ABSOLUTE PATHS)
AMAZON_REPORT_PATH = os.path.join(SCRIPT_DIR, 'Amazon Sale Report.csv')
INTERNATIONAL_REPORT_PATH = os.path.join(SCRIPT_DIR, 'International sale Report.csv')
SALE_REPORT_PATH = os.path.join(SCRIPT_DIR, 'Sale Report.csv')

# Renaming and type conversion dictionaries
AMAZON_RENAME = {
    "Order ID": "order_id", "Date": "order_date", "Qty": "quantity", "Amount": "amount",
    "SKU": "sku", "ship-city": "ship_city", "ship-state": "ship_state",
    "ship-postal-code": "ship_postal_code", "ship-country": "ship_country",
    "Sales Channel": "sales_channel", "Style": "product_style", "Category": "product_category",
    "Size": "product_size", "ASIN": "product_asin", "promotion-ids": "promotion_ids",
    "B2B": "is_b2b", "fulfilled-by": "fulfillment_by", "Courier Status": "courier_status",
    "currency": "currency", "Status": "order_status", "Fulfilment": "fulfillment_type",
    "ship-service-level": "ship_service_level",
}
AMAZON_TYPES = {
    'order_date': lambda x: pd.to_datetime(x, errors='coerce'),
    'ship_postal_code': lambda x: x.astype(str)
}

INTERNATIONAL_RENAME = {
    "DATE": "order_date", "Months": "order_month", "CUSTOMER": "customer_name",
    "Style": "product_style", "SKU": "sku", "Size": "product_size", "PCS": "quantity",
    "RATE": "unit_price", "GROSS AMT": "total_amount",
}
INTERNATIONAL_TYPES = {
    'order_date': lambda x: pd.to_datetime(x, format='%m-%d-%y', errors='coerce'),
    'quantity': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64),
    'unit_price': lambda x: pd.to_numeric(x, errors='coerce'),
    'total_amount': lambda x: pd.to_numeric(x, errors='coerce')
}

SALE_RENAME = {
    "SKU Code": "sku", "Design No.": "design_no", "Stock": "current_stock",
    "Category": "product_category", "Size": "product_size", "Color": "product_color",
}
SALE_TYPES = {
    'current_stock': lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(np.int64)
}


load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),        # No default needed if .env is mandatory
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": quote_plus(os.getenv("DB_PASSWORD")) # Still need to encode!
}
TABLE_NAME = "sales_fact"

# --- ADD THESE DEBUG PRINTS ---
print(f"DEBUG: Raw DB_USER from .env: {os.getenv('DB_USER')}")
print(f"DEBUG: Raw DB_PASSWORD from .env: {os.getenv('DB_PASSWORD')}")
print(f"DEBUG: Password after quote_plus: {DB_CONFIG['password']}")


# --- Helper Function for Data Cleaning ---
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
        logging.debug(f"Dropped columns: {drop_cols}")
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True, errors='ignore')
        logging.debug(f"Renamed columns based on: {rename_dict}")
    if dtype_conversions:
        for col, func in dtype_conversions.items():
            if col in df.columns:
                df[col] = func(df[col])
                logging.debug(f"Converted column '{col}' to specified type.")
    return df

# --- Pipeline Steps as Functions ---

def load_and_clean_source_data():
    """Loads and cleans the Amazon, International, and Sale reports."""
    logging.info("--- Starting Data Loading and Initial Cleaning ---")

    df_amazon = pd.DataFrame()
    df_international = pd.DataFrame()
    df_sale = pd.DataFrame()

    # Load and clean Amazon data
    try:
        logging.info(f"Loading and cleaning '{AMAZON_REPORT_PATH}'...")
        df_amazon = pd.read_csv(AMAZON_REPORT_PATH)
        df_amazon = clean_dataframe(
            df_amazon,
            drop_cols=['Unnamed: 22', 'index'],
            rename_dict=AMAZON_RENAME,
            dtype_conversions=AMAZON_TYPES
        )
        logging.debug(f"Amazon data head:\n{df_amazon.head().to_string()}")
        logging.debug(f"Amazon data info:\n{df_amazon.info()}")
        logging.debug(f"Amazon data missing values:\n{df_amazon.isnull().sum().to_string()}")
        logging.info(f"Successfully processed '{AMAZON_REPORT_PATH}'. Shape: {df_amazon.shape}")
    except FileNotFoundError:
        logging.error(f"Error: '{AMAZON_REPORT_PATH}' not found. Please ensure the file is in the correct directory.")
        sys.exit(1) # Exit if critical file is missing
    except Exception as e:
        logging.error(f"An error occurred while reading '{AMAZON_REPORT_PATH}': {e}")
        sys.exit(1)

    # Load and clean International data
    try:
        logging.info(f"Loading and cleaning '{INTERNATIONAL_REPORT_PATH}'...")
        df_international = pd.read_csv(INTERNATIONAL_REPORT_PATH)
        df_international = clean_dataframe(
            df_international,
            drop_cols=['index'],
            rename_dict=INTERNATIONAL_RENAME,
            dtype_conversions=INTERNATIONAL_TYPES
        )
        logging.debug(f"International data head:\n{df_international.head().to_string()}")
        logging.debug(f"International data info:\n{df_international.info()}")
        logging.debug(f"International data missing values:\n{df_international.isnull().sum().to_string()}")
        logging.info(f"Successfully processed '{INTERNATIONAL_REPORT_PATH}'. Shape: {df_international.shape}")
    except FileNotFoundError:
        logging.error(f"Error: '{INTERNATIONAL_REPORT_PATH}' not found. Please ensure the file is in the correct directory.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred while reading '{INTERNATIONAL_REPORT_PATH}': {e}")
        sys.exit(1)

    # Load and clean Sale data (product master)
    try:
        logging.info(f"Loading and cleaning '{SALE_REPORT_PATH}'...")
        df_sale = pd.read_csv(SALE_REPORT_PATH)
        df_sale = clean_dataframe(
            df_sale,
            drop_cols=['index'],
            rename_dict=SALE_RENAME,
            dtype_conversions=SALE_TYPES
        )
        logging.debug(f"Sale data head:\n{df_sale.head().to_string()}")
        logging.debug(f"Sale data info:\n{df_sale.info()}")

        # Handle duplicate SKUs in Sale Report (product master data)
        initial_sale_rows = len(df_sale)
        initial_unique_skus = df_sale['sku'].nunique()
        logging.info(f"Sale Report - Initial rows: {initial_sale_rows}, Unique SKUs: {initial_unique_skus}")

        df_sale.drop_duplicates(subset='sku', keep='first', inplace=True)
        dropped_skus_count = initial_sale_rows - len(df_sale)
        logging.info(f"Dropped {dropped_skus_count} duplicate SKUs from Sale Report. New rows: {len(df_sale)}")

        if df_sale['sku'].duplicated().any():
            logging.warning("Duplicate SKUs still found in df_sale after dropping. Investigate 'keep' strategy.")
        logging.debug(f"Sale data missing values after deduplication:\n{df_sale.isnull().sum().to_string()}")
        logging.info(f"Successfully processed '{SALE_REPORT_PATH}'. Shape: {df_sale.shape}")

    except FileNotFoundError:
        logging.error(f"Error: '{SALE_REPORT_PATH}' not found. Please ensure the file is in the correct directory.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred while reading '{SALE_REPORT_PATH}': {e}")
        sys.exit(1)

    return df_amazon, df_international, df_sale

def integrate_sales_data(df_amazon, df_international, df_sale):
    """Integrates cleaned sales data and enriches with product master data."""
    logging.info("--- Starting Data Integration ---")

    df_combined_sales = pd.concat([df_amazon, df_international], ignore_index=True)
    logging.info(f"Combined Amazon and International sales. Combined shape: {df_combined_sales.shape}")

    df_final_sales = pd.merge(df_combined_sales, df_sale, on='sku', how='left')
    logging.info(f"Merged combined sales with Sale Report (product master) on 'sku'. Final shape: {df_final_sales.shape}")

    # Refine merged data: handle duplicate column names from merge
    df_final_sales.drop(columns=['product_category_x', 'product_size_x'], inplace=True, errors='ignore')
    df_final_sales.rename(columns={
        'product_category_y': 'product_category',
        'product_size_y': 'product_size'
    }, inplace=True, errors='ignore')
    logging.info("Resolved conflicting column names ('product_category', 'product_size') post-merge.")
    logging.debug(f"Final Sales Data head after merge refinement:\n{df_final_sales.head().to_string()}")
    logging.debug(f"Final Sales Data info after merge refinement:\n{df_final_sales.info()}")
    logging.debug(f"Final Sales Data missing values after merge refinement:\n{df_final_sales.isnull().sum().to_string()}")

    return df_final_sales

def clean_and_qa_final_sales_data(df_final_sales):
    """
    Performs final cleaning and quality assurance checks on the combined sales data.
    Ensures data types are correct, handles missing values, and standardizes channels.
    """
    logging.info("--- Starting Final Cleaning and QA ---")

    # Convert object columns to numeric where appropriate, coercing errors
    numeric_cols = ['quantity', 'unit_price', 'amount', 'total_amount', 'current_stock']
    for col in numeric_cols:
        if col in df_final_sales.columns:
            df_final_sales[col] = pd.to_numeric(df_final_sales[col], errors='coerce')

    # Fill NaN numerical values with 0
    numerical_cols_to_fill_zero = ['amount', 'unit_price', 'total_amount', 'current_stock']
    for col in numerical_cols_to_fill_zero:
        if col in df_final_sales.columns:
            df_final_sales[col] = df_final_sales[col].fillna(0)
    logging.info("Filled NaN numerical values with 0.")

    # Convert date column to datetime objects
    df_final_sales['order_date'] = pd.to_datetime(df_final_sales['order_date'], errors='coerce', dayfirst=True)
    logging.info("Converted 'order_date' to datetime format.")

    # Handle missing date values - dropping rows for now, can be adjusted
    initial_rows = len(df_final_sales)
    df_final_sales.dropna(subset=['order_date'], inplace=True)
    if len(df_final_sales) < initial_rows:
        logging.warning(f"Dropped {initial_rows - len(df_final_sales)} rows due to invalid 'order_date'.")

    # Clean and standardize sales channels
    # First, apply specific channel logic where order_id is NOT NaN
    # (Existing logic, no change needed here)
    df_final_sales['sales_channel'] = np.where(
        df_final_sales['order_id'].notna() & (
            df_final_sales['order_id'].str.startswith('S') |
            df_final_sales['order_id'].str.startswith('D')
        ),
        'Non-Amazon',
        df_final_sales['sales_channel']
    )
    df_final_sales['sales_channel'] = np.where(
        df_final_sales['order_id'].notna() &
        df_final_sales['order_id'].str.startswith('B'),
        'Amazon.in',
        df_final_sales['sales_channel']
    )

    # Generate synthetic order_ids for originally NaN order_ids (international sales without order IDs)
    # This addresses the 'Unknown' order count issue
    logging.info("Generating synthetic order_ids for previously missing order_ids.")
    missing_order_id_mask = df_final_sales['order_id'].isna()

    # Create a unique string for hashing from relevant columns for these rows
    # Include 'CUSTOMER', 'DATE', 'Style', 'SKU' as they seem to identify unique transactions in international data
    # Ensure these columns exist and are handled for NaNs before hashing
    cols_for_hash = ['CUSTOMER', 'DATE', 'Style', 'SKU', 'PCS', 'RATE', 'GROSS AMT'] # Adding more columns for uniqueness
    for col in cols_for_hash:
        if col not in df_final_sales.columns: # Map back to original names if needed
             logging.warning(f"Column '{col}' not found in df_final_sales for hashing. Check mapping in load_and_clean_source_data.")
             # Fallback to an empty string or 'Unknown' if column missing or entirely NaN
             df_final_sales[col] = df_final_sales.get(col, pd.Series(np.nan, index=df_final_sales.index)).fillna('')
        else:
             df_final_sales[col] = df_final_sales[col].fillna('').astype(str) # Ensure string type for hashing


    df_final_sales.loc[missing_order_id_mask, 'order_id'] = df_final_sales[missing_order_id_mask].apply(
        lambda row: 'INT_' + hashlib.md5(
            (str(row['CUSTOMER']) + str(row['DATE']) + str(row['Style']) + str(row['SKU']) + str(row['PCS']) + str(row['RATE']) + str(row['GROSS AMT'])).encode('utf-8')
        ).hexdigest(), axis=1
    )
    logging.info("Generated synthetic order_ids.")

    # Now, fill remaining NaN sales_channels with 'Unknown' after order_id generation
    # This will still assign 'Unknown' to those that don't match Amazon.in or Non-Amazon prefixes
    df_final_sales['sales_channel'] = df_final_sales['sales_channel'].fillna('Unknown')
    logging.info("Filled remaining NaN sales channels with 'Unknown'.")


    # Final check on data types and missing values
    for col in ['sales_channel', 'order_id', 'Style', 'SKU', 'Size', 'customer_id']:
        if col in df_final_sales.columns:
            df_final_sales[col] = df_final_sales[col].fillna('Unknown') # Fill string columns too
    logging.info("Filled NaN string values with 'Unknown'.")


    # Drop any rows that still have critical NaNs after all filling (e.g., if order_id was all NaN and couldn't be hashed)
    initial_rows = len(df_final_sales)
    df_final_sales.dropna(subset=['order_id', 'sales_channel'], inplace=True)
    if len(df_final_sales) < initial_rows:
        logging.warning(f"Dropped {initial_rows - len(df_final_sales)} rows due to critical missing values after filling.")

    logging.debug(f"Final Sales Data head after Cleaning and QA:\n{df_final_sales.head().to_string()}")
    logging.debug(f"Final Sales Data info after Cleaning and QA:\n{df_final_sales.info()}")
    logging.debug(f"Final Sales Data missing values after Cleaning and QA:\n{df_final_sales.isnull().sum().to_string()}")
    logging.info("--- Final Cleaning and QA Complete ---")
    return df_final_sales


def engineer_features(df_final_sales):
    """Engineers time-based and price-related features."""
    logging.info("--- Starting Feature Engineering ---")

    # Time-based features
    df_final_sales['order_year'] = df_final_sales['order_date'].dt.year
    df_final_sales['order_month_num'] = df_final_sales['order_date'].dt.month
    df_final_sales['order_day_of_week'] = df_final_sales['order_date'].dt.dayofweek
    df_final_sales['order_hour'] = df_final_sales['order_date'].dt.hour
    logging.info("Extracted year, month, day of week, and hour from 'order_date'.")

    # Ensure numeric types
    df_final_sales['quantity'] = pd.to_numeric(df_final_sales['quantity'], errors='coerce').fillna(0)
    df_final_sales['unit_price'] = pd.to_numeric(df_final_sales['unit_price'], errors='coerce').fillna(0)
    df_final_sales['amount'] = pd.to_numeric(df_final_sales['amount'], errors='coerce').fillna(0)

    # Calculate total_price without intermediate column
    df_final_sales['total_price'] = np.where(
        df_final_sales['amount'] > 0,
        df_final_sales['amount'],
        np.where(
            df_final_sales['total_amount'] > 0,
            df_final_sales['total_amount'],
            df_final_sales['quantity'] * df_final_sales['unit_price']
        )
    )

    logging.info("Calculated 'total_price' using 'amount' if available, else 'quantity * unit_price'.")

    # Add validation for zero prices
    zero_prices = df_final_sales[
        (df_final_sales['total_price'] == 0) & 
        (df_final_sales['quantity'] > 0)
    ]
    if not zero_prices.empty:
        logging.warning(f"Found {len(zero_prices)} orders with quantity but zero price!")
        logging.debug(zero_prices[['sales_channel', 'sku', 'quantity', 'unit_price', 'amount', 'total_amount', 'total_price']].head())

    logging.debug(f"Final Sales Data head after Feature Engineering:\n{df_final_sales.head().to_string()}")
    logging.debug(f"Final Sales Data info after Feature Engineering:\n{df_final_sales.info()}")
    logging.debug(f"Final Sales Data missing values after Feature Engineering:\n{df_final_sales.isnull().sum().to_string()}")
    logging.info("--- Feature Engineering Complete ---")
    return df_final_sales

# --- Main Pipeline Execution ---
if __name__ == "__main__":
    logging.info("--- Starting E-commerce Data Pipeline ---")

    # Step 1: Load and initial clean of individual reports
    df_amazon_clean, df_international_clean, df_sale_clean = load_and_clean_source_data()

    # Step 2: Integrate sales data
    df_final_sales = integrate_sales_data(df_amazon_clean, df_international_clean, df_sale_clean)

    # Step 3: Perform final cleaning and quality assurance
    df_final_sales = clean_and_qa_final_sales_data(df_final_sales)

    # Step 4: Engineer new features
    df_final_sales = engineer_features(df_final_sales)

    logging.info("--- E-commerce Data Pipeline Completed Successfully ---")
    logging.info(f"Final DataFrame shape: {df_final_sales.shape}")
    logging.info(f"Final DataFrame columns: {list(df_final_sales.columns)}")
    logging.debug(f"Final DataFrame head:\n{df_final_sales.head().to_string()}")
    logging.debug(f"Final DataFrame info:\n{df_final_sales.info()}")
    logging.debug(f"Final DataFrame missing values summary:\n{df_final_sales.isnull().sum().to_string()}")

    # At this point, df_final_sales is ready to be loaded into PostgreSQL
    # Example placeholder for the next step (PostgreSQL loading)
    # logging.info("Next step: Loading df_final_sales into PostgreSQL Data Warehouse.")
    # from your_db_module import load_to_postgresql
    # load_to_postgresql(df_final_sales, 'your_table_name')
    
def load_data_to_postgresql(df, table_name, db_config):
    """
    Loads a Pandas DataFrame into a PostgreSQL table.
    """
    logging.info(f"--- Starting data load into PostgreSQL table: '{table_name}' ---")

    db_url = (
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

    # Define the columns that should be in the final table
    final_columns = [
        'order_id', 'order_date', 'order_status', 'fulfillment_type', 
        'sales_channel', 'ship_service_level', 'product_style', 'sku',
        'product_asin', 'courier_status', 'quantity', 'currency', 
        'amount', 'ship_city', 'ship_state', 'ship_postal_code',
        'ship_country', 'promotion_ids', 'is_b2b', 'fulfillment_by',
        'customer_name', 'unit_price', 'total_amount', 'design_no',
        'current_stock', 'product_category', 'product_size', 'product_color',
        'order_year', 'order_month_num', 'order_day_of_week', 'order_hour',
        'total_price'
    ]

    # Filter DataFrame to only include the columns that exist in PostgreSQL
    df_to_load = df[final_columns].copy()

    try:
        engine = create_engine(db_url)
        logging.info("PostgreSQL engine created successfully.")

        # Use the filtered DataFrame
        df_to_load.to_sql(table_name, engine, if_exists='append', 
                         index=False, chunksize=1000)

        logging.info(f"Successfully loaded {len(df_to_load)} rows into '{table_name}' table.")

    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")
        raise # Re-raise the exception to stop the pipeline if load fails
    finally:
        if 'engine' in locals():
            engine.dispose() # Ensure the connection is closed
            logging.info("PostgreSQL connection closed.")

# --- Main Pipeline Execution (MODIFIED) ---
if __name__ == "__main__":
    logging.info("--- Starting E-commerce Data Pipeline ---")

    try:
        # Step 1: Load and initial clean of individual reports
        df_amazon_clean, df_international_clean, df_sale_clean = load_and_clean_source_data()

        # Step 2: Integrate sales data
        df_final_sales = integrate_sales_data(df_amazon_clean, df_international_clean, df_sale_clean)

        # Step 3: Perform final cleaning and quality assurance
        df_final_sales = clean_and_qa_final_sales_data(df_final_sales)

        # Step 4: Engineer new features
        df_final_sales = engineer_features(df_final_sales)

        logging.info("--- E-commerce Data Pipeline Completed Successfully (Data Transformation Phase) ---")
        logging.info(f"Final DataFrame shape: {df_final_sales.shape}")
        logging.debug(f"Final DataFrame head:\n{df_final_sales.head().to_string()}")

        # Step 5: Load data into PostgreSQL Data Warehouse
        load_data_to_postgresql(df_final_sales, TABLE_NAME, DB_CONFIG)

        logging.info("--- ALL ETL Pipeline steps completed successfully! Data loaded to PostgreSQL. ---")

    except Exception as pipeline_error:
        logging.critical(f"ETL Pipeline encountered a critical error: {pipeline_error}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure
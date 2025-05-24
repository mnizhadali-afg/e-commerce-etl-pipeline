# E-commerce Sales Data Analysis - Lessons Learned, Key Decisions & Best Practices (Data Cleaning Phase)

This document highlights key lessons learned, challenges overcome, important decisions made, and best practices followed during the data loading and cleaning of the e-commerce sales datasets.

---

## 1. Initial Data Discovery: Expect the Unexpected & Informing Decisions

### Challenge: Inconsistent Data Formats & Quality
- **Observation:** We dealt with three distinct datasets (`amazon_sale_report.csv`, `International_sale_report.csv`, `SaleReport.csv`), each with its own structure, column names, and data types. For instance, `order_date` was an object in both sales reports, and some columns in the Amazon report had mixed types (`DtypeWarning`).
- **Lesson Learned:** Always start with a thorough data profiling. Ignoring initial warnings or data type inconsistencies can lead to errors and unreliable analyses downstream.
- **Best Practice Followed:**
    - **Thorough Initial Data Profiling:** Used `.info()`, `.head()`, and `.isnull().sum()` on *each* raw dataset to understand structure, data types, and missing values from the outset. This systematic approach helped identify issues like incorrect `order_date` types and mixed-type columns early on.
- **Key Decision:**
    - **Standardized Date Format:** Decided to convert all `order_date` columns to `datetime64[ns]` across all relevant dataframes (`df_amazon`, `df_international`). This ensures consistent temporal analysis.

### Challenge: Duplicate Product SKUs & Data Integrity
- **Observation:** The `SaleReport.csv` (our product master) contained duplicate `sku` entries, despite `sku` being a presumed unique identifier for products.
- **Lesson Learned:** Never assume uniqueness for identifier columns, even if logically they should be unique. Always verify data integrity.
- **Best Practice Followed:**
    - **Pre-emptive Duplicate Checking:** Systematically used `.duplicated().sum()` on the `sku` column in `df_sale` to identify and quantify duplicates before any merging. This ensures the foundational master data is clean.
- **Key Decision:**
    - **Unique Product Master:** Explicitly decided to drop duplicate `sku` entries in `df_sale`, keeping only the first occurrence. This ensures a clean, unique product master list, which is foundational for accurate sales aggregation.

---

## 2. Merging Complex Datasets: A Stitch in Time Saves Nine

### Challenge: Column Name Discrepancies Across Datasets
- **Observation:** When merging the Amazon and International sales reports, and then with the product master, many column names were similar but not identical, or entirely different (e.g., `amount` vs `total_amount`, or `product_category` appearing in multiple files).
- **Lesson Learned:** Ambiguous or inconsistent column names are a major hurdle in data integration. A clear strategy for column harmonization is crucial.
- **Best Practice Followed:**
    - **Proactive Column Harmonization:** Before merging, explicitly defined and executed renaming strategies for columns (e.g., `total_amount` to `amount`, `product_category_df_sale` to `product_category`) to create a unified schema. This minimized confusion and errors post-merge.
- **Key Decision:**
    - **Standardized Naming Convention for Merged Data:** Renamed columns from `df_international` and `df_sale` to align with the more comprehensive `df_amazon` columns where possible. This creates a more cohesive final dataset suitable for cross-source analysis.

### Challenge: Introducing Missing Values During Merging
- **Observation:** After concatenating and merging the datasets, new `NaN` values appeared in columns that were originally clean in one dataset but missing in another (e.g., `order_id` in International sales records after merging with Amazon data).
- **Lesson Learned:** Merging inherently introduces `NaN` values for data not present in all joined tables. This is a natural consequence, not an error.
- **Best Practice Followed:**
    - **Phased Cleaning Approach:** Implemented a dedicated post-merge cleaning phase to systematically address newly introduced `NaN` values, rather than trying to handle all missing values in one go.

---

## 3. Strategic Missing Value Handling: No One-Size-Fits-All

### Challenge: Diverse Missing Value Scenarios & Impact
- **Observation:** We encountered different types of missing data:
    - Critical identifiers (`sku`, `order_date`) where missing data made the record unusable.
    - Numerical values (`amount`, `unit_price`, `current_stock`) where `NaN`s could break calculations.
    - Categorical/descriptive values (`product_category`, `courier_status`, `promotion_ids`) where `NaN` might just mean "not specified."
    - Boolean values (`is_b2b`) which could have `NaN`.
- **Lesson Learned:** A "one-size-fits-all" approach to missing value imputation is inefficient and can lead to data distortion. Understanding the context of each missing value is paramount.
- **Best Practice Followed:**
    - **Context-Driven Imputation Strategy:** Applied different imputation methods based on column data type and criticality:
        - **Dropping Rows:** For high-criticality missing values (`sku`, `order_date`) where the absence of data renders the record fundamentally unusable for core analysis.
        - **Zero Imputation:** For numerical columns where a missing value logically implies zero (`amount`, `unit_price`, `current_stock`).
        - **'Unknown'/'N/A' Imputation:** For categorical/descriptive columns where data absence doesn't invalidate the record but needs to be explicitly marked (`product_category`, `courier_status`, and merged-in `order_id`, `ship_city`, etc.).
        - **Logical Boolean Imputation:** Explicitly converted `is_b2b` `NaN`s to `False`, assuming non-B2B if unspecified.
- **Key Decision:**
    - **Balanced Data Retention vs. Integrity:** A conscious decision was made to drop rows only for the most critical missing values (`sku`, `order_date`) to preserve the majority of the data. For other columns, imputation was chosen to maximize the usable dataset size for diverse analytical questions.

### Challenge: `FutureWarning: A value is trying to be set on a copy...`
- **Observation:** Repeated warnings about `inplace=True` when modifying DataFrames, indicating potential issues in future Pandas versions.
- **Lesson Learned:** Relying on deprecated functionalities can lead to breaking code in future library updates. It's important to keep code future-proof.
- **Best Practice Followed:**
    - **Avoiding `inplace=True`:** Acknowledged the warning and made a mental note (for future code refactoring, or immediately if project constraints allow) to avoid `inplace=True` by reassigning columns (e.g., `df['col'] = df['col'].fillna('Unknown')`) for safer and more explicit operations.

---

## Conclusion: Clean Data is the Foundation
The entire data cleaning pipeline, from initial exploration to strategic missing value handling and intelligent merging, was instrumental. By meticulously following these best practices and making informed decisions, we transformed raw, disparate datasets into a robust, clean `df_final_sales` DataFrame, now perfectly poised for meaningful insights and advanced analytics.
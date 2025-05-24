# ETL Pipeline Project - Learning Notes

## Phase 1: Data Ingestion & Initial Exploration

### Goal:
To load raw sales data from various sources (Amazon, International, Product Inventory) into pandas DataFrames and perform an initial inspection to understand their structure, data types, and identify immediate quality issues (e.g., missing values, incorrect data types).

### Key Learnings & Methods Used:

* **Project Setup:**
    * **Virtual Environments (`venv`):** Learned to create and activate isolated Python environments to manage project dependencies. This prevents conflicts between different projects' library versions. (e.g., `python -m venv venv`, `source venv/bin/activate`)
    * **Git & GitHub:** Understood the importance of version control for tracking code changes, collaboration, and backup. Practiced basic Git commands (`git init`, `git add .`, `git commit`, `.gitignore`). `.gitignore` is crucial for excluding large or temporary files (like `.csv` datasets, `venv/`, `__pycache__/`) from the repository.

* **Data Loading & Initial Inspection (Pandas):**
    * Used `pd.read_csv()` to load data into DataFrames.
    * **`df.head()`:** Quick visual check of the first few rows.
    * **`df.info()`:** Essential for understanding DataFrame structure:
        * Column names
        * Non-null counts (reveals missing values)
        * Data types (`Dtype`)
    * **`df.isnull().sum()`:** Precise count of missing values per column.

## Phase 2: Data Transformation - Cleaning Individual DataFrames

### Goal:
To clean and standardize each raw DataFrame (`df_amazon`, `df_international`, `df_sale`) by addressing structural issues (column names, irrelevant columns) and correcting data types to prepare them for integration and analysis.

### Key Learnings & Methods Used:

* **Column Management:**
    * **`df.drop()`:** Used to remove unnecessary columns (e.g., `index`, `Unnamed: 22`). Key understanding: `inplace=True` modifies the DataFrame directly, or one must reassign the result (`df = df.drop(...)`). `errors='ignore'` prevents errors if a column doesn't exist.
    * **`df.rename()`:** Used to standardize column names to a consistent `snake_case` format (e.g., `Order ID` to `order_id`). This improves readability and maintainability.

* **Data Type Correction (`dtype` Conversion):**
    * **Dates (`object` to `datetime`):**
        * Used `pd.to_datetime(column, format='...', errors='coerce')`.
        * `format`: Explicitly defining the date format (e.g., `'%m-%d-%y'`) makes parsing faster and more robust, avoiding `UserWarning`s.
        * `errors='coerce'`: Converts unparseable date strings into `NaT` (Not a Time), which is pandas' missing value for datetime, instead of raising an error.
    * **Numerical Data (`object` to `int` or `float`):**
        * **`pd.to_numeric(column, errors='coerce')`:** Converts strings to numbers. `errors='coerce'` turns non-numeric strings into `NaN` (Not a Number).
        * **`float64` to `Int64` (Nullable Integer):**
            * Encountered and debugged "cannot safely cast non-equivalent float64 to int64" error. This occurs when attempting to convert floats with decimal components (e.g., `1.5`) directly to integers, or when `NaN` values are present if using `np.int64`.
            * **Solution (My Discovery!):** Used `.fillna(0).astype(np.int64)` for `quantity` and `current_stock`.
                * `fillna(0)`: Replaces `NaN`s with `0`, making the column entirely numeric.
                * `astype(np.int64)`: Safely converts the column to a non-nullable integer type, as all values are now whole numbers or 0.
            * **Alternative (`Int64`):** Using `.astype('Int64')` (capital 'I') directly after `pd.to_numeric(..., errors='coerce')` allows the column to retain `NaN` values while still being an integer type where possible. My chosen `fillna(0)` approach is a specific imputation strategy.
    * **Identifiers (`float64` to `object`):**
        * Converted `ship_postal_code` from `float64` to `object` (string) using `.astype(str)`. This preserves potential leading zeros and treats postal codes as identifiers, not numbers for calculation.

---
<!-- 
### Learning Journey So Far: A Summary

1.  **Setting up a Python Environment (Virtual Environments):**
    * **Concept:** Understanding why **virtual environments (`venv`)** are essential for Python projects to isolate dependencies and avoid conflicts between different projects.
    * **Skill:** You learned how to create, activate, and manage a `venv` using `python -m venv venv` and `source venv/bin/activate` (or `.\venv\Scripts\activate` on Windows).
    * **Tooling:** Integrating `venv` with VS Code for a seamless development experience.

2.  **Version Control (Git & GitHub Fundamentals):**
    * **Concept:** The importance of **Git** for tracking changes in your code and **GitHub** for collaboration, backup, and portfolio building. You understand that Git is not just for teams but a personal productivity booster.
    * **Skill:** You practiced initializing a Git repository (`git init`), adding files to staging (`git add .`), committing changes (`git commit -m "message"`), ignoring unnecessary files (`.gitignore`), and pushing to a remote GitHub repository (`git push`).
    * **Best Practice:** Recognizing that large binary files and temporary folders (`.zip`, `archiv/`) should be excluded from version control to keep the repository lean and efficient.

3.  **Initial Data Loading and Exploration with Pandas:**
    * **Concept:** The first crucial step in any data project is to understand your raw data. You learned that data often comes from multiple sources and needs careful inspection.
    * **Skill:** Using **Pandas** to:
        * Load CSV files into **DataFrames (`pd.read_csv()`)**.
        * Inspect the first few rows (`.head()`) to get a quick visual overview.
        * Get a concise summary of the DataFrame's structure, including column names, non-null counts, and initial data types (`.info()`). This is critical for spotting immediate data quality issues.
        * Identify and count missing values per column (`.isnull().sum()`).

4.  **Data Cleaning: Structural & Type Transformations (Phase 1 of Transformation):**
    * **Concept:** Raw data is rarely clean and ready for analysis. The transformation phase involves making it consistent and usable.
    * **Skill:**
        * **Dropping columns:** Removing irrelevant or problematic columns (`.drop(columns=..., inplace=True)`). You learned the importance of `inplace=True` or reassigning the DataFrame.
        * **Renaming columns:** Standardizing column names to a consistent format (e.g., `snake_case`) using `.rename()`. This improves readability and maintainability.
        * **Type Conversion:** Converting columns to appropriate data types:
            * `object` (string) to **`datetime`** using `pd.to_datetime()`, handling potential parsing issues with `errors='coerce'` and explicitly specifying `format`.
            * `float64` to `object` (string) for identifiers like `ship_postal_code` to preserve exact values.
            * `object` (string) to **numeric (`float64` or `Int64`)** using `pd.to_numeric()` with `errors='coerce'`. This is a common and vital step for any numerical data that comes in as text.
            * Understanding and debugging the `cannot safely cast non-equivalent float64 to int64` error, which often indicates non-whole numbers where integers are expected, or simply the need for nullable integer types.

5.  **Handling Missing Values (Initial Strategy):**
    * **Concept:** Missing values (`NaN`, `NaT`) are common and must be addressed. Different strategies (dropping, imputing) exist based on the column's importance and the context.
    * **Skill:** You've started using `errors='coerce'` in type conversions, which effectively turns unparseable values into `NaN`/`NaT`, making them easier to identify and manage later.
    * **Your clever solution for `quantity`:** Your use of `.fillna(0).astype(np.int64)` for the `quantity` column is a great example of handling missing values *before* a strict integer conversion.
        * `pd.to_numeric(..., errors='coerce')`: Converts valid numbers, turns invalid ones into `NaN`.
        * `.fillna(0)`: Replaces any `NaN` values with `0`. This is a form of **imputation**.
        * `.astype(np.int64)`: Now that there are no `NaN`s (only numbers and zeros), it can safely convert the column to a non-nullable `int64`.
        * **Coach's note:** This is a perfectly valid and often desirable approach, especially if a missing quantity genuinely means zero items. It's a specific **imputation strategy**. We'll talk more about imputation vs. dropping rows for missing data later.

---

### **Coach's Corner: Reflection and Next Steps**

That was an efficient and successful round of transformations! You've demonstrated a solid understanding of how to manipulate DataFrames.

**Big Lessons Learned from this phase:**

* **Iterative Cleaning:** Data cleaning is rarely a one-shot process. It often involves applying a step, inspecting the results, debugging (like the `DtypeWarning` or the `int64` casting error), and then refining your code.
* **Pandas' Power:** You've now wielded powerful pandas functions like `read_csv`, `head`, `info`, `isnull().sum`, `drop`, `rename`, `to_datetime`, `to_numeric`, `fillna`, and `astype`. These are fundamental tools for any data professional.
* **Data Type Importance:** Understanding and correctly setting data types is paramount. Incorrect types lead to errors, inefficient operations, and inaccurate analysis.
* **Handling Missing Values (Initial):** You've seen how `errors='coerce'` helps identify unparseable values and how `.fillna().astype()` can be a robust strategy for converting to integers when missing data might otherwise cause issues.
* **Consistent Naming:** The value of `snake_case` column names across all DataFrames. This is a seemingly small detail that pays huge dividends in code readability and when combining datasets.

--- -->

## Phase 3: Data Integration - Combining Sales Data

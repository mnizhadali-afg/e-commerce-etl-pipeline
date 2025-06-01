# ETL Pipeline Project Notes

## Project Overview
**Description:** Development of a Python-based ETL pipeline for extracting sales data from various sources, transforming it for analysis, and loading it into a PostgreSQL Data Warehouse. [cite: 1]
**Technologies:** Python (Pandas, SQLAlchemy, hashlib), PostgreSQL, SQL.

## I. Data Source & Preparation (ET - Extract & Transform)

### 1. Data Ingestion & Initial Cleaning
* **Sources:** Amazon Sale Report (`Amazon Sale Report.csv`), International Sale Report (`International sale Report.csv`), Product Master (`Sale Report.csv`).
* **Initial Steps:**
    * Loaded data using `pandas.read_csv`.
    * Applied a `clean_dataframe` helper function to:
        * Strip whitespace from column names.
        * Drop irrelevant columns (`Unnamed: 22`, `index`).
        * Rename columns for consistency (e.g., "Order ID" to "order_id" for Amazon, "PCS" to "quantity" for International)[cite: 1].
        * **Important Note:** The `International sale Report.csv` originally lacked an "Order ID" column. This led to `NaN` values for `order_id` in records from this source[cite: 1].
        * Convert data types (e.g., `order_date` to datetime, `ship_postal_code` to string, numerical columns to appropriate types).
    * **Crucial for Merge:** Ensured `df_sale` (Product Master) had **unique SKUs** by dropping duplicates based on the `sku` column (`df_sale.drop_duplicates(subset='sku', keep='first', inplace=True)`). This was vital for a successful merge. [cite: 1]

### 2. Data Integration
* Combined Amazon and International sales data using `pd.concat`[cite: 1].
* Enriched the combined sales data by performing a **`LEFT MERGE`** with the cleaned `df_sale` (Product Master) on the `sku` column[cite: 1].
* Resolved column name conflicts (e.g., `product_category_x`, `product_category_y`) by dropping redundant columns and renaming the preferred ones[cite: 1].

### 3. Final Data Cleaning & Quality Assurance
* **Missing Values Handling:**
    * **Dropped rows** for critical missing identifiers: `sku` and `order_date`.
    * **Filled with `0`** for numerical columns where missing implied no value (e.g., `amount`, `unit_price`, `total_amount`, `current_stock`)[cite: 1].
    * **Filled with `'Unknown'`** for categorical columns where missing implied unknown category/value (e.g., `product_category`, `product_size`, `customer_name` etc.)[cite: 1].
* **Type Conversions:** Ensured `is_b2b` was a boolean type. [cite: 1]
* **`sales_channel` Derivation Logic:**
    * `Amazon.in`: Assigned if `order_id` starts with 'B'. [cite: 1]
    * `Non-Amazon`: Assigned if `order_id` starts with 'S' or 'D'. [cite: 1]
    * `Unknown`: Assigned to records where `sales_channel` is still `NaN` after the above rules.
* **Synthetic `order_id` Generation (for `Unknown` channel resolution):**
    * **Problem:** Original `International sale Report.csv` lacked an `Order ID` column. This resulted in `NaN` `order_id` values, causing all international sales to be categorized as `Unknown` channel and incorrectly counted as `1` distinct order[cite: 1].
    * **Resolution:** For rows with initially missing `order_id`, a unique, synthetic `order_id` is generated using `hashlib.md5` on a combination of existing columns (`CUSTOMER`, `DATE`, `Style`, `SKU`, `PCS`, `RATE`, `GROSS AMT`). This allows for proper counting of distinct international transactions. [cite: 1]
* **Robustness:** Added checks for `NaN` after operations and handled potential `DtypeWarning` during CSV reads. [cite: 1]

### 4. Feature Engineering
* **Time-Based Features:** Extracted `order_year`, `order_month_num`, `order_day_of_week`, and `order_hour` from the `order_date` column. [cite: 1]
* **Calculated Metrics (`total_price` - **Crucial Fix**):**
    * **Problem:** Initial `total_price` calculation (e.g., `quantity * unit_price`) resulted in $0.00 for `Amazon.in` sales, despite valid sales data in the `Amount` column[cite: 1]. Similarly, `Non-Amazon` sales also showed $0.00.
    * **Resolution:** The `total_price` calculation was made more robust by prioritizing data from available columns:
        1.  If `amount` (from Amazon data) is positive, `total_price` uses `amount`.
        2.  Else, if `total_amount` (from International data) is positive, `total_price` uses `total_amount`.
        3.  Else, `total_price` falls back to `quantity * unit_price`. [cite: 1]
    * **Result:** `Amazon.in` sales are now correctly aggregated (e.g., ~$78.5M)[cite: 1]. `Unknown` sales amounts are also correctly captured.

---

## II. PostgreSQL Data Warehouse Setup & Loading (L - Load)

### 1. PostgreSQL Installation & Environment
* **PostgreSQL Server:** Installed globally on Ubuntu using `sudo apt install postgresql postgresql-contrib`. This is a system service, not a Python venv package.
* **Python Libraries:** `psycopg2-binary` and `SQLAlchemy` are installed within the Python virtual environment (`venv`) using `pip install`. These enable Python to connect to PostgreSQL.
    * Confirmed installation using `pip list` or observing "Requirement already satisfied" messages.

### 2. PostgreSQL Server Configuration & User Management
* **Service Status Check:** Verified PostgreSQL server status with `sudo systemctl status postgresql`.
* **Authentication Issue (`pg_hba.conf`):**
    * **Problem:** Default `peer` authentication for local Unix socket connections (`local all all peer`) and potentially `ident` for local TCP/IP connections prevented `etl_user` from connecting properly[cite: 1].
    * **Solution:** Edited `pg_hba.conf` (typically `/etc/postgresql/<version>/main/pg_hba.conf`) to change the authentication method to `md5` or `scram-sha-256` for both `local` (Unix socket) and `host` (TCP/IP 127.0.0.1/32) connections. [cite: 1]
    * **Action:** Changed the lines `local all all md5` and `host all all 127.0.0.1/32 md5` (or `scram-sha-256`).
    * **Crucial Step:** Restarted PostgreSQL service after modification: `sudo systemctl restart postgresql`.
* **Database and User Creation:**
    * Connected as superuser: `sudo -u postgres psql`.
    * Created database: `CREATE DATABASE ecommerce_dw;`.
    * Created user: `CREATE USER etl_user WITH PASSWORD 'your_secure_password';`.
    * Granted privileges: `GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO etl_user;`.

### 3. Schema & Table Creation (`sales_fact`)
* **Permission Denied for Schema Public (during `CREATE TABLE`):**
    * **Problem:** Even after granting `CREATE` permission on the `public` schema, `etl_user` could not create tables because the `postgres` user still *owned* the `public` schema, implicitly restricting some operations.
    * **Solution:** Explicitly changed the ownership of the `public` schema to `etl_user`.
    * **Action:** Connected as `postgres` to the specific database (`sudo -u postgres psql -d ecommerce_dw`) and ran: `ALTER SCHEMA public OWNER TO etl_user;`.
    * **Other Essential Grants (for robust permissions):**
        * `GRANT CREATE ON SCHEMA public TO etl_user;`
        * `GRANT USAGE ON SCHEMA public TO etl_user;`
        * `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO etl_user;` (for future objects)
* **Table Definition (SQL):** Defined the `sales_fact` table mirroring the DataFrame columns with appropriate PostgreSQL data types.
    * `CREATE TABLE IF NOT EXISTS sales_fact (...)`. This was run successfully after ensuring `etl_user` had necessary permissions.

### 4. Python-to-PostgreSQL Data Loading
* **Connection String Construction:** Used `sqlalchemy.create_engine` to build the database connection URL.
* **Special Characters in Password:**
    * **Problem:** Passwords containing special characters (like `@`) can break the URL format, causing "could not translate host name" errors. Also, inline comments in `.env` were incorrectly parsed as part of the password string[cite: 1].
    * **Solution:** URL-encode the password using `urllib.parse.quote_plus`. Ensured `.env` file entries were clean, with comments on separate lines, and each value terminated by a newline. [cite: 1]
* **Data Transfer:** Utilized `pandas.DataFrame.to_sql` to efficiently load the `df_final_sales` DataFrame into the `sales_fact` table.
    * **Idempotency:** `if_exists='append'` is used, but for static source data, this leads to duplication upon re-runs. For testing and clean loads, `TRUNCATE TABLE sales_fact;` was used before each run.
    * `index=False` prevents writing the DataFrame index as a column.
    * `chunksize=1000` was used for performance optimization on large datasets.
* **Security (Best Practice):** Emphasized using environment variables (`os.getenv`) loaded from a `.env` file (with `python-dotenv`) for database credentials. This keeps sensitive data out of source code and allows for flexible environment management.

---

## III. Pipeline Automation

* **Tool:** `cron` (time-based job scheduler on Unix-like systems).
* **Objective:** To automatically execute the `etl_pipeline.py` script at regular intervals.
* **Key Steps:**
    * **Make script executable:** Added `#!/usr/bin/env python3` as the first line of `etl_pipeline.py` and ran `chmod +x etl_pipeline.py`.
    * **Use Absolute Paths for Data Files:** Modified the Python script to use `os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filename.csv')` for source CSVs. This ensures the script can find data files regardless of the cron job's working directory.
    * **Configure Cron Job:** Edited user's crontab (`crontab -e`) to add a line for scheduling.
        * **Example Cron Entry:** `0 3 * * * /path/to/your/venv/bin/python /path/to/your/etl_pipeline.py >> /path/to/your/etl_pipeline_cron.log 2>&1`
        * **Absolute Paths are Critical:** Ensured the full path to the virtual environment's Python interpreter and the script itself are specified.
        * **Logging:** Redirected all script output (stdout and stderr) to a dedicated log file (`>> logfile 2>&1`) for monitoring and debugging.
* **Persistence:** Confirmed that cron jobs run in the background as a system daemon and are not tied to an open terminal session.
* **Rationale for Automation (even with static data):** While not strictly necessary for static source files after the initial load, automation is crucial for learning robust data engineering practices, preparing for future incremental data, and simulating real-world dynamic data pipelines.
* **Multiple Cron Jobs for One Pipeline:** Generally discouraged for a single, sequentially dependent pipeline due to challenges in dependency management and error handling. A single cron job for the entire `etl_pipeline.py` is the appropriate approach.

---

## IV. Next Steps (Future Considerations)

The core ETL pipeline is now functional and accurately loads the data, reflecting the quality of the source files. The data in the warehouse is ready for analysis, with specific considerations:

1.  **Understand Source Data Limitations:**
    * **`Non-Amazon` Channel:** The `total_sales_amount` for `Non-Amazon` remains $0.00. This is a direct consequence of the `Amazon Sale Report.csv` having **empty `amount` and `currency` fields** for records with 'S' or 'D' prefixed `Order ID`s, which are categorized as `Non-Amazon`[cite: 1]. To gain insights into `Non-Amazon` sales revenue, a cleaner source or data imputation strategy would be required.
    * **`Unknown` Channel:** The `total_sales_amount` is correctly calculated for the `Unknown` channel, and the `total_orders` now reflects a more accurate count due to synthetic `order_id` generation.

2.  **Define and Analyze E-commerce Metrics:** Focus on metrics that can be reliably derived from the current data (e.g., `Amazon.in` sales, trends over time, top products based on available `quantity` and `total_price`).
3.  **Advanced Data Modeling:** Consider star/snowflake schema for a more normalized Data Warehouse structure.
4.  **Advanced Orchestration:** For very complex pipelines, explore tools like Apache Airflow, Prefect, or Dagster for more robust scheduling, monitoring, and dependency management beyond `cron`.
5.  **Monitoring & Alerting:** Implement logging and monitoring for pipeline health and data quality.
6.  **Data Validation:** Add more comprehensive data validation steps after loading to ensure data integrity in the DWH.
7.  **Reporting & Visualization:** Connect the `ecommerce_dw` to BI tools (Metabase, Power BI, Tableau, etc.) for dashboarding and reporting on sales trends and insights.
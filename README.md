# ETL Pipeline Project Notes

## Project Overview
**Description:** Development of a Python-based ETL pipeline for extracting sales data from various sources, transforming it for analysis, and loading it into a PostgreSQL Data Warehouse.
**Technologies:** Python (Pandas, SQLAlchemy), PostgreSQL, SQL.

## I. Data Source & Preparation (ET - Extract & Transform)

### 1. Data Ingestion & Initial Cleaning
* **Sources:** Amazon Sale Report (`Amazon Sale Report.csv`), International Sale Report (`International sale Report.csv`), Product Master (`Sale Report.csv`).
* **Initial Steps:**
    * Loaded data using `pandas.read_csv`.
    * Applied a `clean_dataframe` helper function to:
        * Strip whitespace from column names.
        * Drop irrelevant columns (`Unnamed: 22`, `index`).
        * Rename columns for consistency (e.g., "Order ID" to "order_id").
        * Convert data types (e.g., `order_date` to datetime, `ship_postal_code` to string, numerical columns to appropriate types).
    * **Crucial for Merge:** Ensured `df_sale` (Product Master) had **unique SKUs** by dropping duplicates based on the `sku` column (`df_sale.drop_duplicates(subset='sku', keep='first', inplace=True)`). This was vital for a successful merge.

### 2. Data Integration
* Combined Amazon and International sales data using `pd.concat`.
* Enriched the combined sales data by performing a **`LEFT MERGE`** with the cleaned `df_sale` (Product Master) on the `sku` column.
* Resolved column name conflicts (e.g., `product_category_x`, `product_category_y`) by dropping redundant columns and renaming the preferred ones.

### 3. Final Data Cleaning & Quality Assurance
* **Missing Values Handling:**
    * **Dropped rows** for critical missing identifiers: `sku` and `order_date`.
    * **Filled with `0`** for numerical columns where missing implied no value (e.g., `amount`, `unit_price`, `total_amount`, `current_stock`).
    * **Filled with `'Unknown'`** for categorical columns where missing implied unknown category/value (e.g., `product_category`, `product_size`, `customer_name`, `order_id` etc.).
* **Type Conversions:** Ensured `is_b2b` was a boolean type.
* **Robustness:** Added checks for `NaN` after operations and handled potential `DtypeWarning` during CSV reads.

### 4. Feature Engineering
* **Time-Based Features:** Extracted `order_year`, `order_month_num`, `order_day_of_week`, and `order_hour` from the `order_date` column.
* **Calculated Metrics:** Derived `total_price` as `quantity * unit_price`.

---

## II. PostgreSQL Data Warehouse Setup & Loading (L - Load)

### 1. PostgreSQL Installation & Environment
* **PostgreSQL Server:** Installed globally on Ubuntu using `sudo apt install postgresql postgresql-contrib`. This is a system service, not a Python venv package.
* **Python Libraries:** `psycopg2-binary` and `SQLAlchemy` are installed within the Python virtual environment (`venv`) using `pip install`. These enable Python to connect to PostgreSQL.
    * Confirmed installation using `pip list` or observing "Requirement already satisfied" messages.

### 2. PostgreSQL Server Configuration & User Management
* **Service Status Check:** Verified PostgreSQL server status with `sudo systemctl status postgresql`.
* **Peer Authentication Issue (`pg_hba.conf`):**
    * **Problem:** Default `peer` authentication for local Unix socket connections (`local all all peer`) prevented direct connection as a specific database user (e.g., `etl_user`) if the system user didn't match.
    * **Solution:** Edited `pg_hba.conf` (typically `/etc/postgresql/<version>/main/pg_hba.conf`) to change `peer` to `md5` or `scram-sha-256` for `local` connections.
    * **Action:** Changed the line `local all all peer` to `local all all md5` (or `scram-sha-256`).
    * **Crucial Step:** Restarted PostgreSQL service after modification: `sudo systemctl restart postgresql`.
* **Database and User Creation:**
    * Connected as superuser: `sudo -u postgres psql`.
    * Created database: `CREATE DATABASE ecommerce_dw;`.
    * Created user: `CREATE USER etl_user WITH PASSWORD 'your_secure_password';`.
    * Granted privileges: `GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO etl_user;`.

### 3. Schema & Table Creation (`sales_fact`)
* **Permission Denied for Schema Public (during `CREATE TABLE`):**
    * **Problem:** Even after granting `CREATE` permission on the `public` schema, `etl_user` could not create tables because the `postgres` user still *owned* the `public` schema, leading to implicit restrictions.
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
    * **Problem:** Passwords containing special characters (like `@`) can break the URL format, causing "could not translate host name" errors.
    * **Solution:** URL-encode the password using `urllib.parse.quote_plus`. This function (from Python's standard library) converts special characters into their safe, percent-encoded equivalents.
    * **Example:** `quote_plus("your!P@ssword")`
* **Data Transfer:** Utilized `pandas.DataFrame.to_sql` to efficiently load the `df_final_sales` DataFrame into the `sales_fact` table.
    * `if_exists='append'` used to add new data to existing table.
    * `index=False` prevents writing the DataFrame index as a column.
    * `chunksize=1000` was used for performance optimization on large datasets.
* **Successful Load:** Confirmed that `146193` rows were successfully loaded into the `sales_fact` table, matching the DataFrame size.
* **Security (Future Best Practice):** Emphasized using environment variables (`os.getenv`) loaded from a `.env` file (with `python-dotenv`) for database credentials. This keeps sensitive data out of source code and allows for flexible environment management.

---

## III. Next Steps (Future Considerations)

Now that the core ETL is functional and data is loaded:

1.  **Automate the Pipeline Execution (Scheduling):** Set up routine runs (e.g., daily) using tools like `cron` on Ubuntu for basic automation, or more advanced orchestrators like Apache Airflow for complex workflows.
2.  **Define and Analyze E-commerce Metrics:** Start extracting meaningful insights from the loaded data using SQL queries, creating PostgreSQL views, or connecting Business Intelligence (BI) tools.

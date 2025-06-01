# PostgreSQL SQL Queries Used in ETL Project

This document lists the key SQL queries executed during the setup, development, and debugging of the ETL pipeline in PostgreSQL.

## 1. Database and User Setup

These queries are executed as the `postgres` superuser (e.g., `sudo -u postgres psql`).

* **Create the database:**
    ```sql
    CREATE DATABASE ecommerce_dw;
    ```

* **Create the ETL user:**
    ```sql
    CREATE USER etl_user WITH PASSWORD 'your_secure_password';
    ```

* **Grant all privileges on the database to the ETL user:**
    ```sql
    GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO etl_user;
    ```

* **Change ownership of the `public` schema (Crucial for permissions):**
    * _Note: Connect to the specific database first, e.g., `sudo -u postgres psql -d ecommerce_dw`_
    ```sql
    ALTER SCHEMA public OWNER TO etl_user;
    ```

* **Grant `CREATE` and `USAGE` on the `public` schema to the ETL user:**
    ```sql
    GRANT CREATE ON SCHEMA public TO etl_user;
    GRANT USAGE ON SCHEMA public TO etl_user;
    ```

* **Alter default privileges for future tables created by `etl_user` in `public` schema:**
    ```sql
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO etl_user;
    ```

## 2. Table Management

These queries are executed by the `etl_user` (either directly in `psql` after connecting as `etl_user`, or via the Python script).

* **Create the `sales_fact` table:**
    ```sql
    CREATE TABLE IF NOT EXISTS sales_fact (
        order_id TEXT PRIMARY KEY,
        order_date DATE,
        sales_channel TEXT,
        customer_id TEXT,
        style TEXT,
        sku TEXT,
        size TEXT,
        quantity INTEGER,
        unit_price NUMERIC,
        amount NUMERIC,
        total_amount NUMERIC,
        total_price NUMERIC,
        current_stock NUMERIC
    );
    ```
    _Note: The `PRIMARY KEY (order_id)` constraint was added for data integrity in the `notes.md` file. In the actual `etl_pipeline.py` script, `order_id` may not be guaranteed unique from combined sources unless explicitly handled for all cases._

* **Truncate the `sales_fact` table (for clean re-loads in ETL):**
    ```sql
    TRUNCATE TABLE sales_fact;
    ```

## 3. Data Inspection and Debugging

These queries were crucial for understanding the data state and diagnosing issues within the ETL pipeline.

* **Get top sales by sales channel (main analytical query):**
    ```sql
    SELECT
        sales_channel,
        SUM(total_price) AS total_sales_amount,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_quantity_sold
    FROM
        sales_fact
    GROUP BY
        sales_channel
    ORDER BY
        total_sales_amount DESC;
    ```

* **Inspect `Non-Amazon` sales details (for debugging $0.00 issue):**
    ```sql
    SELECT
        order_id,
        sales_channel,
        quantity,
        unit_price,
        amount,
        total_price
    FROM
        sales_fact
    WHERE
        sales_channel = 'Non-Amazon'
    LIMIT 10;
    ```

* **Generic inspection of loaded data:**
    ```sql
    SELECT * FROM sales_fact LIMIT 10;
    ```